package de.tuberlin.campus.dwbi.jobs;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * This class demonstrates how to accomplish task 1 (Pre-processing)
 * (Not actually needed for ECLAT algorithm implementation)
 * <p>
 * "To start your mining you need to reduce the data set so you
 * have all products purchased in one transaction together. Additionally, implement
 * a mapping for the products, e.g. milk -> 1, bread -> 2, etc."
 */
public class ExampleMappingJob {

    private final static String INPUT_FILE = "./src/main/resources/OnlineRetail_short.csv";
    private static SortedMap<Integer, String> idItemMap;

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, SortedSet<Integer>>> tidProducts = env
                .readCsvFile(INPUT_FILE)
                .fieldDelimiter(",")
                .includeFields("11000000")
                .ignoreFirstLine()
                .types(String.class, String.class)
                .groupBy(0)
                .reduceGroup(new TransactionReducer())
                .setParallelism(1);

        tidProducts.print();

        idItemMap.forEach((k, v) -> System.out.println(k + " -> " + v));
    }

    public static void setMapping(SortedMap<Integer, String> map) {
        idItemMap = map;
    }
}

class TransactionReducer extends RichGroupReduceFunction<Tuple2<String, String>, Tuple2<String, SortedSet<Integer>>> {

    private SortedMap<Integer, String> idItemMap = new TreeMap<>();
    private int id = 0;

    @Override
    public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, SortedSet<Integer>>> out) throws Exception {

        Iterator<Tuple2<String, String>> tuples = values.iterator();

        Tuple2<String, String> tuple = tuples.next();
        String tid = tuple.f0;

        SortedSet<Integer> set = new TreeSet<>();
        set.add(++id);
        idItemMap.put(id, tuple.f1);

        while (tuples.hasNext()) {
            tuple = tuples.next();
            set.add(++id);
            idItemMap.put(id, tuple.f1);
        }

        out.collect(new Tuple2<>(tid, set));
    }

    @Override
    public void close() throws Exception {
        ExampleMappingJob.setMapping(idItemMap);
        super.close();
    }
}
