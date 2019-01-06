package de.tuberlin.campus.dwbi.jobs;

import de.tuberlin.campus.dwbi.functions.crosses.PairsCross;
import de.tuberlin.campus.dwbi.functions.crosses.TriplesCross;
import de.tuberlin.campus.dwbi.functions.filters.SizeFilter;
import de.tuberlin.campus.dwbi.functions.filters.SupportFilter;
import de.tuberlin.campus.dwbi.functions.keyselectors.ItemSetKeySelector;
import de.tuberlin.campus.dwbi.functions.mappers.ProductIdMapper;
import de.tuberlin.campus.dwbi.functions.mappers.CountSupportMapper;
import de.tuberlin.campus.dwbi.functions.reducers.TransactionsProductsReducer;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.SortedSet;

public class ECLATJob {

    private final static String INPUT_FILE = "./src/main/resources/online_retail.csv";
    private final static String OUTPUT_FILE = "./src/main/resources/output.txt";
    private final static int MIN_SUPPORT = 300;

    public static void main(String[] args) throws Exception {

        // Track runtime
        long startTime = System.currentTimeMillis();

        // Print info for ongoing execution
        printStartupInfo();

        // Create Flink execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Extract 'InvoiceNo' and 'StockCode' from CSV data
        // and map it to: <(String) StockCode -> (SortedSet) [InvoiceNo, InvoiceNo, ...]>
        DataSet<Tuple2<String, SortedSet<String>>> productsTransactions = env
                .readCsvFile(INPUT_FILE)
                .fieldDelimiter(",")
                .includeFields("11000000")
                .ignoreFirstLine()
                .types(String.class, String.class)
                .groupBy(1)
                .reduceGroup(new TransactionsProductsReducer());

        // Map product identifier (String) to an id (int) for better performance
        // Note: setParallelism(1) is necessary due to incremental product id!
        DataSet<Tuple2<Integer, SortedSet<String>>> singles = productsTransactions
                .map(new ProductIdMapper())
                .setParallelism(1)
                .filter(new SupportFilter<>(MIN_SUPPORT));

        // Find frequent pairs
        DataSet<Tuple2<SortedSet<Integer>, SortedSet<String>>> pairs = singles
                .cross(singles)
                .with(new PairsCross())
                .filter(new SizeFilter(2))
                .distinct(new ItemSetKeySelector())
                .filter(new SupportFilter<>(MIN_SUPPORT));

        // Find frequent sets of three items and count total number of occurrence
        DataSet<Tuple3<SortedSet<Integer>, Integer, SortedSet<String>>> triples = pairs
                .cross(pairs)
                .with(new TriplesCross())
                .filter(new SizeFilter(3))
                .distinct(new ItemSetKeySelector())
                .filter(new SupportFilter<>(MIN_SUPPORT))
                .map(new CountSupportMapper());

        // Save results to file
        triples.sortPartition(1, Order.DESCENDING)
                .writeAsText(OUTPUT_FILE, FileSystem.WriteMode.OVERWRITE);

        // Execute Flink job
        env.execute("Flink ECLAT Job");

        // Calculate and print runtime in seconds
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("\nTook time: " + (double) totalTime / 1000 + " s");

        // Print the number of frequent item sets
        System.out.println("Frequent item sets found: " + Files.lines(Paths.get(OUTPUT_FILE)).count());
    }

    private static void printStartupInfo() {

        // Print info for ongoing execution
        System.out.println("Flink ECLAT Job started with following config: ");
        System.out.println("  INPUT: " + INPUT_FILE);
        System.out.println("  OUTPUT: " + OUTPUT_FILE);
        System.out.println("  MIN_SUPPORT: " + MIN_SUPPORT);
    }
}
