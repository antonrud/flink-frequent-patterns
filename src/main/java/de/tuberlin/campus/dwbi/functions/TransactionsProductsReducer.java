package de.tuberlin.campus.dwbi.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class TransactionsProductsReducer implements GroupReduceFunction<Tuple2<Integer, String>, Tuple2<String, Set<Integer>>> {

    private Set<Integer> transactions;

    @Override
    public void reduce(Iterable<Tuple2<Integer, String>> positions, Collector<Tuple2<String, Set<Integer>>> collector) throws Exception {

        transactions = new HashSet<>();

        positions.forEach(p -> transactions.add(p.f0));

        collector.collect(new Tuple2<>(positions.iterator().next().f1, transactions));
    }
}
