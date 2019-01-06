package de.tuberlin.campus.dwbi.jobs;

import de.tuberlin.campus.dwbi.functions.crosses.PairsCross;
import de.tuberlin.campus.dwbi.functions.crosses.TriplesCross;
import de.tuberlin.campus.dwbi.functions.filters.SizeFilter;
import de.tuberlin.campus.dwbi.functions.filters.SupportFilter;
import de.tuberlin.campus.dwbi.functions.keyselectors.ItemSetKeySelector;
import de.tuberlin.campus.dwbi.functions.mappers.ProductIdMapper;
import de.tuberlin.campus.dwbi.functions.reducers.TransactionsProductsReducer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.SortedSet;

public class ECLATJob {

    //private final static String CSV_FILE = "./src/main/resources/online_retail.csv";
    //private final static double MIN_SUPPORT = 100;
    private final static String CSV_FILE = "./src/main/resources/OnlineRetail_short.csv";
    private final static int MIN_SUPPORT = 2;

    public static void main(String[] args) throws Exception {

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
                .setParallelism(1);
        
        DataSet<Tuple2<SortedSet<Integer>, SortedSet<String>>> pairs = idsTransactions
                .cross(idsTransactions)
                .with(new PairsCross())
                .filter(new SizeFilter(2))
                .distinct(new ItemSetKeySelector())
                .filter(new SupportFilter<>(MIN_SUPPORT));

        DataSet<Tuple2<SortedSet<Integer>, SortedSet<String>>> triples = pairs
                .cross(pairs)
                .with(new TriplesCross())
                .filter(new SizeFilter(3))
                .distinct(new ItemSetKeySelector())
                .filter(new SupportFilter<>(MIN_SUPPORT));

//        idsTransactions.filter(new SupportFilter<>()).print();
        pairs.print();

        /* Execute program with sink to file
        itemsQuantity.sortPartition(2, Order.DESCENDING)
                .setParallelism(1)
                .writeAsCsv("./src/main/resources/quantity.csv", FileSystem.WriteMode.OVERWRITE);

        env.execute("Flink Batch Job");
        */
    }
}
