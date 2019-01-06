package de.tuberlin.campus.dwbi.functions.filters;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.SortedSet;

public class SizeFilter implements FilterFunction<Tuple2<SortedSet<Integer>, SortedSet<String>>> {

    private int size;

    public SizeFilter(int size) {

        this.size = size;
    }

    @Override
    public boolean filter(Tuple2<SortedSet<Integer>, SortedSet<String>> value) throws Exception {

        return value.f0.size() == size;
    }
}
