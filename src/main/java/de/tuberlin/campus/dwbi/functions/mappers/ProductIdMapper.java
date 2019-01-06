package de.tuberlin.campus.dwbi.functions.mappers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.SortedSet;

public class ProductIdMapper implements MapFunction<Tuple2<String, SortedSet<String>>, Tuple2<Integer, SortedSet<String>>> {

    private int id = 0;

    @Override
    public Tuple2<Integer, SortedSet<String>> map(Tuple2<String, SortedSet<String>> tuple) throws Exception {

        return new Tuple2<>(++id, tuple.f1);
    }
}
