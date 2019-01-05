package de.tuberlin.campus.dwbi.functions;

import de.tuberlin.campus.dwbi.jobs.ECLATJob;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.SortedSet;

public class SupportFilter<T> implements FilterFunction<Tuple2<T, SortedSet<String>>> {

    @Override
    public boolean filter(Tuple2<T, SortedSet<String>> tuple) throws Exception {

        return tuple.f1.size() >= ECLATJob.MIN_SUPPORT;
    }
}
