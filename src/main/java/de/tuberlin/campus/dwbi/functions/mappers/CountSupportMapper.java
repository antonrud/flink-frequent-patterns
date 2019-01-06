package de.tuberlin.campus.dwbi.functions.mappers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.SortedSet;

public class CountSupportMapper implements MapFunction<Tuple2<SortedSet<Integer>, SortedSet<String>>, Tuple3<SortedSet<Integer>, Integer, SortedSet<String>>> {
    @Override
    public Tuple3<SortedSet<Integer>, Integer, SortedSet<String>> map(Tuple2<SortedSet<Integer>, SortedSet<String>> value) throws Exception {

        return new Tuple3<>(value.f0, value.f1.size(), value.f1);
    }
}
