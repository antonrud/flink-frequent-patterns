package de.tuberlin.campus.dwbi.data;

import java.util.HashSet;
import java.util.Set;

public class ProductTransactionsItem {

    private String id;
    private Set<Integer> transactions;

    public ProductTransactionsItem(String id) {

        this.id = id;
        this.transactions = new HashSet<>();
    }

    public void addTransaction(int tid) {

        this.transactions.add(tid);
    }

    public String getId() {

        return id;
    }

    public Set<Integer> getTransactions() {

        return transactions;
    }
}
