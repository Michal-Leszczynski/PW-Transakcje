package cp1.solution;

import cp1.base.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class TM implements cp1.base.TransactionManager {

    // statyczna mapa okreslajaca, czy dany watek posiada rozpoczeta transakcje na dowolnym TM
    private static volatile ConcurrentHashMap<Long, Boolean> active_transactions_global = new ConcurrentHashMap<>();

    // narzedzie sluzace do pozyskania czasu rozpoczecia transakcji
    private volatile LocalTimeProvider timeProvider;

    // mapa okreslajaca zasoby kontrolowane przez MT
    // (ulatwia ich dostepnosc)
    public volatile ConcurrentHashMap<ResourceId, Resource> resources;

    // mapa okreslajaca id watkow, ktore kontroluja dany zasob
    private volatile ConcurrentHashMap<ResourceId, Long> resource_holder;

    // mapa okreslajaca transakcje o danym id w danym TM
    private volatile ConcurrentHashMap<Long, Transaction> active_transactions;

    // mapa semaforow sluzacych do czekania na dany zasob
    private volatile ConcurrentHashMap<ResourceId, Semaphore> waiting_line;

    // mapa okreslajaca, ile transakcji czeka na zasob o dany id
    private volatile ConcurrentHashMap<ResourceId, Integer> how_many_waits;

    // kolejka id zasobow, ktore zwalnia transakcja wykonujaca
    // rollbackCurrentTransaction() lub commitCurrentTransaction()
    private volatile BlockingQueue<ResourceId> to_be_freed;

    // licznik okreslajacy rozmiar to_be_freed
    private volatile AtomicInteger how_many_to_be_freed;

    // mutex broniacy wstepu do sekcji krytycznej
    private volatile Semaphore mutex;

    // id watku, ktory potencjalnie nalezy anulowac
    private volatile long thread_id_to_be_aborted;

    // konstruktor
    public TM(Collection<Resource> resources, LocalTimeProvider timeProvider) {
        this.timeProvider = timeProvider;

        this.resources = new ConcurrentHashMap<>();
        this.resource_holder = new ConcurrentHashMap<>();
        this.active_transactions = new ConcurrentHashMap<>();
        this.waiting_line = new ConcurrentHashMap<>();
        this.how_many_waits = new ConcurrentHashMap<>();
        this.to_be_freed = new LinkedBlockingQueue<>();
        this.how_many_to_be_freed = new AtomicInteger(0);

        this.mutex = new Semaphore(1, true);

        for (Resource resource : resources) {
            this.resources.put(resource.getId(), resource);
            this.waiting_line.put(resource.getId(), new Semaphore(0, true));
            this.how_many_waits.put(resource.getId(), 0);
        }
    }

    // podnosci mutex (nie rzuca przy tym wyjatkiem InterruptedException)
    private void take_mutex_without_exc() {
        long id = Thread.currentThread().getId();
        Thread.interrupted();

        try {
            mutex.acquire();
        } catch (InterruptedException e) {
            System.out.println("Unexpected error!!!");
        }
    }

    // zdejmuje kolejna wartosc z kolejki to_be_freed
    // (nie rzuca przy tym wyjatkiem InterruptedException)
    private ResourceId take_to_be_freed_without_exc() {
        long id = Thread.currentThread().getId();
        Thread.interrupted();

        try {
            return to_be_freed.take();
        } catch (InterruptedException e) {
            System.out.println("Unexpected error!!!");

            return null;
        }
    }

    // funkcja pomocnicza zmieniajaca liczbe czekajacych
    // watkow na dany zasob
    private void change_how_many_waits(ResourceId rid, int delta) {
        int temp = how_many_waits.get(rid);
        how_many_waits.put(rid, temp + delta);
    }

    // funkcja pomocnicza zwalniajaca zasoby kontrolowane przez
    // transakcje o danym id
    // jej wywolanie znajduje sie pod ochrona mutex'a
    private void free_resources(long id) {
        ArrayList<ResourceId> temp_arr = active_transactions.get(id).get_controlled_resources();

        for (ResourceId temp_rid : temp_arr) {
            resource_holder.remove(temp_rid);

            // uzupelnianie kolejki zasobow na ktorych zwolnienie
            // czekaja inne transakcje
            if (how_many_waits.get(temp_rid) != 0) {
                to_be_freed.add(temp_rid);
                how_many_to_be_freed.incrementAndGet();
            }
        }

        // poczatek sekwencyjnego budzenia watkow, ktore
        // oczekuja na zasoby o id z to_be_freed
        // dziedziczenie sekcji krytycznej
        if (how_many_to_be_freed.get() != 0) {
            ResourceId temp_rid = take_to_be_freed_without_exc();

            waiting_line.get(temp_rid).release();
        } else {
            mutex.release();
        }
    }

    // funkcja pomocnicza cofajaca wszystkie operacje
    // wykonane przez transakcje o danym id
    // jej wywolanie znajduje sie pod ochrona mutex'a
    private void undo_transaction(long id) {
        ArrayList<ResourceOperation> temp_op_arr = active_transactions.get(id).get_transaction_history_operations();
        ArrayList<ResourceId> temp_rid_arr = active_transactions.get(id).get_transaction_history_rid();
        int size = temp_op_arr.size();

        for (int i = size - 1; i >= 0; i--) {
            temp_op_arr.get(i).undo(resources.get(temp_rid_arr.get(i)));
        }
    }

    // funkcja pomocnicza sprawdzajaca, czy oczekiwanie transakcji o danym id
    // na zasob o danym rid doprowadzi do zakleszczenia
    // ustawia zmienna thread_id_to_be_aborted na id transakcji
    // o najpozniejszym czasie rozpoczecia (lub id w drugiej kolejnosci)
    private boolean check_for_deadlock(long id, ResourceId rid) {
        long biggest_starting_time = active_transactions.get(id).get_starting_time();
        thread_id_to_be_aborted = id;

        long overlooked_thread = resource_holder.get(rid);

        while (true) {
            if (overlooked_thread == id) {
                return true;
            }

            long overlooked_thread_starting_time = active_transactions.get(overlooked_thread).get_starting_time();

            // aktualizacja najpozniej rozpoczetej transakcji z potencjalnego cyklu
            if (overlooked_thread_starting_time > biggest_starting_time ||
                    (overlooked_thread_starting_time == biggest_starting_time &&
                    thread_id_to_be_aborted < overlooked_thread)) {

                biggest_starting_time = overlooked_thread_starting_time;
                thread_id_to_be_aborted = overlooked_thread;
            }

            ResourceId temp_rid = active_transactions.get(overlooked_thread).get_waiting_for_resource();

            if (temp_rid == null) {
                return false;
            }

            overlooked_thread = resource_holder.get(temp_rid);
        }
    }

    // rozpoczecie transakcji
    @Override
    public void startTransaction() throws
            AnotherTransactionActiveException {

        take_mutex_without_exc();

        long time = timeProvider.getTime();
        long id = Thread.currentThread().getId();

        if (active_transactions_global.containsKey(id)) {
            throw new AnotherTransactionActiveException();
        }

        active_transactions.put(id, new Transaction(time, Thread.currentThread()));
        active_transactions_global.put(id, true);

        mutex.release();
    }

    // wykonanie danej operacji na danym zasobie w danym TM
    @Override
    public void operateOnResourceInCurrentTransaction(ResourceId rid, ResourceOperation operation) throws
            NoActiveTransactionException,
            UnknownResourceIdException,
            ActiveTransactionAborted,
            ResourceOperationException,
            InterruptedException {

        take_mutex_without_exc();

        long id = Thread.currentThread().getId();

        // sprawdzanie wyjatkow

        if (!isTransactionActive()) {
            mutex.release();

            throw new NoActiveTransactionException();
        }

        if (!resources.containsKey(rid)) {
            mutex.release();

            throw new UnknownResourceIdException(rid);
        }

        if (isTransactionAborted()) {
            mutex.release();

            throw new ActiveTransactionAborted();
        }

        boolean should_take_resource = false;

        if (resource_holder.containsKey(rid)) {
            if (!(resource_holder.get(rid) == id)) {

                // sprawdzenie, czy nalezy anulowac jakas transakcje
                boolean result = check_for_deadlock(id, rid);

                if (result) {
                    active_transactions.get(thread_id_to_be_aborted).abort();
                }

                if (active_transactions.get(id).is_aborted()) {
                    mutex.release();

                    throw new ActiveTransactionAborted();
                }

                change_how_many_waits(rid, 1);
                active_transactions.get(id).set_waiting_for_resource(rid);

                // interruptowanie anulowanej transakcji
                if (result) {
                    ResourceId aborted_rid = active_transactions.get(thread_id_to_be_aborted).get_waiting_for_resource();
                    change_how_many_waits(aborted_rid, -1);
                    active_transactions.get(thread_id_to_be_aborted).delete_waiting_for_resource();
                    active_transactions.get(thread_id_to_be_aborted).get_thread().interrupt();
                }

                mutex.release();

                // zawieszenie transakcji na semaforze okreslajacym zasob
                // na ktorego dostepnosc czeka
                // normalne wznowienie nastepuje pod ochrona mutex'a
                // dziedziczenie sekcji krytycznej
                try {
                    waiting_line.get(rid).acquire();
                    change_how_many_waits(rid, -1);
                    how_many_to_be_freed.decrementAndGet();
                    active_transactions.get(id).delete_waiting_for_resource();
                }
                // obsluga interruptowanej transakcji
                catch (InterruptedException e) {
                    throw new ActiveTransactionAborted();
                }

                should_take_resource = true;
            }
        } else {
            should_take_resource = true;
        }

        // przyznawanie dostepu do zasobu
        if (should_take_resource) {
            resource_holder.put(rid, id);
            active_transactions.get(id).add_controlled_resource(rid);
        }

        // sekwencyjne budzenie kolejnych watkow
        // czekajacych na zasoby o id z to_be_freed
        if (how_many_to_be_freed.get() != 0) {
            ResourceId temp_rid = take_to_be_freed_without_exc();

            waiting_line.get(temp_rid).release();
        } else {
            mutex.release();
        }

        // wykonanie danej operacji na danym zasobie
        operation.execute(resources.get(rid));

        take_mutex_without_exc();

        // sprawdzenie, czy watek nie zostal zinterruptowany
        // podczas wykonywania operacji na zasobie
        if (Thread.currentThread().isInterrupted()) {
            operation.undo(resources.get(rid));
            mutex.release();

            throw new InterruptedException();
        }

        // aktualizowanie historii transakcji
        active_transactions.get(id).add_operation_on_resource(rid, operation);

        mutex.release();
    }

    // commmitowanie aktywnej transakcji
    @Override
    public void commitCurrentTransaction() throws
            NoActiveTransactionException,
            ActiveTransactionAborted {

        take_mutex_without_exc();

        long id = Thread.currentThread().getId();

        if (!isTransactionActive()) {
            throw new NoActiveTransactionException();
        }

        if (isTransactionAborted()) {
            throw new ActiveTransactionAborted();
        }

        free_resources(id);

        active_transactions.remove(id);
        active_transactions_global.remove(id);
    }

    // rollback aktywnej transakcji
    @Override
    public void rollbackCurrentTransaction() {
        take_mutex_without_exc();

        long id = Thread.currentThread().getId();

        if (isTransactionActive()) {
            undo_transaction(id);
            free_resources(id);

            active_transactions.remove(id);
            active_transactions_global.remove(id);
        } else {
            mutex.release();
        }
    }

    // sprawdzenie, czy transakcja jest aktywna
    @Override
    public boolean isTransactionActive() {
        long id = Thread.currentThread().getId();

        return active_transactions.containsKey(id);
    }

    // sprawdzanie, czy transakcja zostala anulowana
    @Override
    public boolean isTransactionAborted() {
        long id = Thread.currentThread().getId();

        if (isTransactionActive()) {
            return active_transactions.get(id).is_aborted();
        } else {
            return false;
        }
    }
}
