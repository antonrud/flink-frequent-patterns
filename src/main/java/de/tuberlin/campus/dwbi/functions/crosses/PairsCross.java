package de.tuberlin.campus.dwbi.functions.crosses;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.SortedSet;
import java.util.TreeSet;

public class PairsCross implements CrossFunction<Tuple2<Integer, SortedSet<String>>, Tuple2<Integer, SortedSet<String>>, Tuple2<SortedSet<Integer>, SortedSet<String>>> {

    @Override
    public Tuple2<SortedSet<Integer>, SortedSet<String>> cross(Tuple2<Integer, SortedSet<String>> val1, Tuple2<Integer, SortedSet<String>> val2) throws Exception {

        SortedSet<Integer> pair = new TreeSet<>();
        pair.add(val1.f0);
        pair.add(val2.f0);

        // Performs intersection
        val1.f1.retainAll(val2.f1);

        return new Tuple2<>(pair, val1.f1);
    }
}
