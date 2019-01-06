package de.tuberlin.campus.dwbi.functions.keyselectors;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.SortedSet;

public class ItemSetKeySelector implements KeySelector<Tuple2<SortedSet<Integer>, SortedSet<String>>, String> {

    @Override
    public String getKey(Tuple2<SortedSet<Integer>, SortedSet<String>> value) throws Exception {

        return value.f0.toString();
    }
}
