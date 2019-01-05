package de.tuberlin.campus.dwbi.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

public class IntersectionMapper extends RichFlatMapFunction<Tuple2<Integer, SortedSet<String>>, Tuple2<SortedSet<Integer>, SortedSet<String>>> {

    private Collection<Tuple2<Integer, SortedSet<String>>> transactions;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.transactions = getRuntimeContext().getBroadcastVariable("transactions");
    }

    @Override
    public void flatMap(Tuple2<Integer, SortedSet<String>> value, Collector<Tuple2<SortedSet<Integer>, SortedSet<String>>> out) throws Exception {

        for (Tuple2<Integer, SortedSet<String>> transaction : transactions) {
            System.out.println("" + transaction.f0 + " - " + transaction.f1);

            SortedSet<Integer> idSet = new TreeSet<>();
            idSet.add(value.f0);
            idSet.add(transaction.f0);

            transaction.f1.retainAll(value.f1);

            out.collect(new Tuple2<>(idSet, transaction.f1));
        }
    }
}
