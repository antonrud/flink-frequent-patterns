package de.tuberlin.campus.dwbi.jobs;

import de.tuberlin.campus.dwbi.functions.TransactionsProductsReducer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.Set;

public class ECLATJob {


    // Use 'online_retail.csv' file in final version
    //private final static String CSV_FILE = "./src/main/resources/OnlineRetail_short.csv";
    private final static String CSV_FILE = "./src/main/resources/test.csv";

    private final static double SUPPORT_THRESHOLD = 0.5;

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, Set<Integer>>> productsTransactions = env
                .readCsvFile(CSV_FILE)
                .fieldDelimiter(",")
                .includeFields("11000000")
                .ignoreFirstLine()
                .types(Integer.class, String.class)
                .groupBy(1)
                .reduceGroup(new TransactionsProductsReducer());


        /* Execute program with sink to file
        itemsQuantity.sortPartition(2, Order.DESCENDING)
                .setParallelism(1)
                .writeAsCsv("./src/main/resources/quantity.csv", FileSystem.WriteMode.OVERWRITE);

        env.execute("Flink Batch Job");
        */
    }
}
