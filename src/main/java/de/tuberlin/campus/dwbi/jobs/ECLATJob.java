package de.tuberlin.campus.dwbi.jobs;

import de.tuberlin.campus.dwbi.functions.crosses.PairsCross;
import de.tuberlin.campus.dwbi.functions.crosses.TriplesCross;
import de.tuberlin.campus.dwbi.functions.filters.SizeFilter;
import de.tuberlin.campus.dwbi.functions.filters.SupportFilter;
import de.tuberlin.campus.dwbi.functions.keyselectors.ItemSetKeySelector;
import de.tuberlin.campus.dwbi.functions.mappers.CountSupportMapper;
import de.tuberlin.campus.dwbi.functions.mappers.ProductIdMapper;
import de.tuberlin.campus.dwbi.functions.reducers.TransactionsProductsReducer;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class ECLATJob {

    private final static String INPUT_FILE = "./src/main/resources/online_retail.csv";
    private final static String RESULT_FILE = "./src/main/resources/result.txt";
    private final static String MAPPING_FILE = "./src/main/resources/mapping.txt";

    private final static int MIN_SUPPORT = 400;

    // Saves mapping for: Id (int) -> Item (String)
    public static SortedMap<Integer, String> idItemMap = new TreeMap<>();

    public static void main(String[] args) throws Exception {

        // Track runtime
        long startTime = System.currentTimeMillis();

        // Print info about ongoing execution
        printStartupInfo();

        // Create Flink execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

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

        // Find frequent sets of three items
        DataSet<Tuple2<SortedSet<Integer>, SortedSet<String>>> triples = pairs
                .cross(pairs)
                .with(new TriplesCross())
                .filter(new SizeFilter(3))
                .distinct(new ItemSetKeySelector())
                .filter(new SupportFilter<>(MIN_SUPPORT));

        // Count transactions pro frequent set and save results to file
        // Note: setParallelism(1) is necessary to write results in one file!
        triples.map(new CountSupportMapper())
                .sortPartition(1, Order.DESCENDING)
                .writeAsText(RESULT_FILE, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        // Execute Flink job
        env.execute("Flink ECLAT Job");

        // Calculate and print runtime in seconds
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("\nTook time: " + (double) totalTime / 1000 + " s");

        // Save Id -> Item mapping
        saveMapping();

        // Print the number of frequent item sets
        System.out.println("Frequent item sets found: " + Files.lines(Paths.get(RESULT_FILE)).count());
    }

    private static void printStartupInfo() {

        // Print info for ongoing execution
        System.out.println("Flink ECLAT Job started with following config: ");
        System.out.println("  INPUT: " + INPUT_FILE);
        System.out.println("  RESULT: " + RESULT_FILE);
        System.out.println("  MAPPING: " + MAPPING_FILE);
        System.out.println("\n  MIN_SUPPORT: " + MIN_SUPPORT);
    }

    private static void saveMapping() throws IOException {

        List<String> idItemList = new ArrayList<>();
        idItemMap.forEach((key, value) -> idItemList.add(key + " -> " + value));

        Files.write(Paths.get(MAPPING_FILE), (Iterable<String>) idItemList.stream()::iterator, StandardOpenOption.CREATE);
    }
}
