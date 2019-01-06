package de.tuberlin.campus.dwbi.functions;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.SortedSet;

public class TriplesCross implements CrossFunction<Tuple2<SortedSet<Integer>, SortedSet<String>>, Tuple2<SortedSet<Integer>, SortedSet<String>>, Tuple2<SortedSet<Integer>, SortedSet<String>>> {

    @Override
    public Tuple2<SortedSet<Integer>, SortedSet<String>> cross(Tuple2<SortedSet<Integer>, SortedSet<String>> val1, Tuple2<SortedSet<Integer>, SortedSet<String>> val2) throws Exception {

        val1.f0.addAll(val2.f0);
        val1.f1.retainAll(val2.f1);

        return new Tuple2<>(val1.f0, val1.f1);
    }
}
