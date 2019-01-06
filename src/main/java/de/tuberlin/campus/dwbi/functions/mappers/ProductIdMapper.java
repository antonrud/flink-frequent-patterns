package de.tuberlin.campus.dwbi.functions.mappers;

import de.tuberlin.campus.dwbi.jobs.ECLATJob;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

public class ProductIdMapper extends RichMapFunction<Tuple2<String, SortedSet<String>>, Tuple2<Integer, SortedSet<String>>> {

    private SortedMap<Integer, String> idItemMap = new TreeMap<>();
    private int id = 0;

    @Override
    public Tuple2<Integer, SortedSet<String>> map(Tuple2<String, SortedSet<String>> tuple) throws Exception {

        // Save assigned item id
        idItemMap.put(++id, tuple.f0);

        return new Tuple2<>(id, tuple.f1);
    }

    @Override
    public void close() throws Exception {

        ECLATJob.setMapping(idItemMap);

        super.close();
    }
}
