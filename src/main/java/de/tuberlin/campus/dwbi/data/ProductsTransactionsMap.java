package de.tuberlin.campus.dwbi.data;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ProductsTransactionsMap {

    private Map<String, Set<Integer>> productsTransactionsMap;

    public ProductsTransactionsMap() {

        this.productsTransactionsMap = new HashMap<>();
    }

    public void addTransaction(String product, int transaction) {

        productsTransactionsMap.put(product, updateSet(product, transaction));
    }

    private Set<Integer> updateSet(String product, int transaction) {

        Set<Integer> updatedSet = productsTransactionsMap.get(product);
        updatedSet.add(transaction);

        return updatedSet;
    }

    public Set<Integer> getTransactions(String product) {

        return productsTransactionsMap.get(product);
    }

    public Map<String, Set<Integer>> getProductsTransactionsMap() {

        return productsTransactionsMap;
    }
}
