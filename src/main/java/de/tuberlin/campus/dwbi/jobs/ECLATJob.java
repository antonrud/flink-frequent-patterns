package de.tuberlin.campus.dwbi.jobs;

import de.tuberlin.campus.dwbi.functions.crosses.PairsCross;
import de.tuberlin.campus.dwbi.functions.crosses.TriplesCross;
import de.tuberlin.campus.dwbi.functions.filters.SizeFilter;
import de.tuberlin.campus.dwbi.functions.filters.SupportFilter;
import de.tuberlin.campus.dwbi.functions.keyselectors.ItemSetKeySelector;
import de.tuberlin.campus.dwbi.functions.mappers.ProductIdMapper;
import de.tuberlin.campus.dwbi.functions.mappers.CountSupportMapper;
import de.tuberlin.campus.dwbi.functions.reducers.TransactionsProductsReducer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.SortedSet;

public class ECLATJob {

    private final static String CSV_FILE = "./src/main/resources/online_retail.csv";
    //private final static String CSV_FILE = "./src/main/resources/OnlineRetail_short.csv";

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        //final int MIN_SUPPORT = (int) (Files.lines(Paths.get(CSV_FILE)).count() * 0.001);
        final int MIN_SUPPORT = 200;

        System.out.println("Starting execution with MIN_SUPPORT: " + MIN_SUPPORT);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Extract 'InvoiceNo' and 'StockCode' from CSV data
        // and map it to: <(String) StockCode -> (SortedSet) [InvoiceNo, InvoiceNo, ...]>
        DataSet<Tuple2<String, SortedSet<String>>> productsTransactions = env
                .readCsvFile(CSV_FILE)
                .fieldDelimiter(",")
                .includeFields("11000000")
                .ignoreFirstLine()
                .types(String.class, String.class)
                .groupBy(1)
                .reduceGroup(new TransactionsProductsReducer());

        // Map product identifier (String) to an id (int) for better performance
        // Note: setParallelism(1) is necessary due to incremental product id!
        DataSet<Tuple2<Integer, SortedSet<String>>> idsTransactions = productsTransactions
                .map(new ProductIdMapper())
                .setParallelism(1)
                .filter(new SupportFilter<>(MIN_SUPPORT));

        DataSet<Tuple2<SortedSet<Integer>, SortedSet<String>>> pairs = idsTransactions
                .cross(idsTransactions)
                .with(new PairsCross())
                .filter(new SizeFilter(2))
                .distinct(new ItemSetKeySelector())
                .filter(new SupportFilter<>(MIN_SUPPORT));

        DataSet<Tuple3<SortedSet<Integer>, Integer, SortedSet<String>>> triples = pairs
                .cross(pairs)
                .with(new TriplesCross())
                .filter(new SizeFilter(3))
                .distinct(new ItemSetKeySelector())
                .filter(new SupportFilter<>(MIN_SUPPORT))
                .map(new CountSupportMapper());

        triples.print();

        /* Execute program with sink to file
        itemsQuantity.sortPartition(2, Order.DESCENDING)
                .setParallelism(1)
                .writeAsCsv("./src/main/resources/quantity.csv", FileSystem.WriteMode.OVERWRITE);

        env.execute("Flink Batch Job");
        */

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("\nTook time: " + (double) totalTime / 1000 + " s");
    }
}
