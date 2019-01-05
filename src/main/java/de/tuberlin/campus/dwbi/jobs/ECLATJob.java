package de.tuberlin.campus.dwbi.jobs;

import de.tuberlin.campus.dwbi.functions.IntersectionMapper;
import de.tuberlin.campus.dwbi.functions.ProductIdMapper;
import de.tuberlin.campus.dwbi.functions.SupportFilter;
import de.tuberlin.campus.dwbi.functions.TransactionsProductsReducer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.SortedSet;

public class ECLATJob {

    //private final static String CSV_FILE = "./src/main/resources/online_retail.csv";
    //private final static double MIN_SUPPORT = 100;
    private final static String CSV_FILE = "./src/main/resources/OnlineRetail_short.csv";
    public final static double MIN_SUPPORT = 1;

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

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

        DataSet<Tuple2<SortedSet<Integer>, SortedSet<String>>> lol = idsTransactions
                .flatMap(new IntersectionMapper())
                .withBroadcastSet(idsTransactions, "transactions")
                .setParallelism(1);


        idsTransactions.filter(new SupportFilter<>()).print();
        System.out.println();
        lol.filter(new SupportFilter<>()).print();


        /* Execute program with sink to file
        itemsQuantity.sortPartition(2, Order.DESCENDING)
                .setParallelism(1)
                .writeAsCsv("./src/main/resources/quantity.csv", FileSystem.WriteMode.OVERWRITE);

        env.execute("Flink Batch Job");
        */
    }
}
