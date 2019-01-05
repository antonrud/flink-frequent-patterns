package de.tuberlin.campus.dwbi.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

public class TransactionsProductsReducer implements GroupReduceFunction<Tuple2<String, String>, Tuple2<String, SortedSet<String>>> {

    @Override
    public void reduce(Iterable<Tuple2<String, String>> positions, Collector<Tuple2<String, SortedSet<String>>> collector) throws Exception {

        Iterator<Tuple2<String, String>> tuples = positions.iterator();
        Tuple2<String, String> tuple = tuples.next();

        String product = tuple.f1;

        SortedSet<String> transactions = new TreeSet<>();
        transactions.add(tuple.f0);
        while (tuples.hasNext()) {
            tuple = tuples.next();
            transactions.add(tuple.f0);
        }

        collector.collect(new Tuple2<>(product, transactions));
    }
}
