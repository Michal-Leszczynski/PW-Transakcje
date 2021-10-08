package cp1.solution;

import cp1.base.ResourceId;
import cp1.base.ResourceOperation;

import java.util.ArrayList;

public class Transaction {

    // Thread, ktory obsluguje dana transakcje
    private Thread thread;

    // historia kolejnych id zasobow, na ktorych transakcja wykonywala operacje
    private ArrayList<ResourceId> transaction_history_rid;

    // historia kolejnych operacji, ktore wykonywala transakcja
    private ArrayList<ResourceOperation> transaction_history_operations;

    // lista id kontrolowanych zasobow
    private ArrayList<ResourceId> controlled_resources;

    // id zasobu, na ktory czeka transakcja
    private ResourceId waiting_for_resource;

    // czas rozpoczecia transakcji
    private long starting_time;

    // zmienna logiczna okreslajaca, czy transakcja jest anulowana
    private boolean is_aborted;

    // konstruktor
    public Transaction(long starting_time, Thread thread) {
        this.thread = thread;
        this.starting_time = starting_time;

        this.transaction_history_rid = new ArrayList<>();
        this.transaction_history_operations = new ArrayList<>();
        this.controlled_resources = new ArrayList<>();
        this.waiting_for_resource = null;
        this.is_aborted = false;
    }

    // gettery

    public Thread get_thread() {
        return this.thread;
    }

    public ArrayList<ResourceId> get_transaction_history_rid() {
        return this.transaction_history_rid;
    }

    public ArrayList<ResourceOperation> get_transaction_history_operations() {
        return this.transaction_history_operations;
    }

    public ArrayList<ResourceId> get_controlled_resources() {
        return this.controlled_resources;
    }

    public ResourceId get_waiting_for_resource() {
        return this.waiting_for_resource;
    }

    public long get_starting_time() {
        return this.starting_time;
    }

    public boolean is_aborted() {
        return this.is_aborted;
    }

    // dodanie do historii wykonania danej operacji na danym zasobie
    public void add_operation_on_resource(ResourceId rid, ResourceOperation operation) {
        this.transaction_history_rid.add(rid);
        this.transaction_history_operations.add(operation);
    }

    // modyfikatory atrybutow

    public void add_controlled_resource(ResourceId rid) {
        this.controlled_resources.add(rid);
    }

    public void set_waiting_for_resource(ResourceId rid) {
        this.waiting_for_resource = rid;
    }

    public void delete_waiting_for_resource() {
        this.waiting_for_resource = null;
    }

    public void abort() {
        this.is_aborted = true;
    }
}
