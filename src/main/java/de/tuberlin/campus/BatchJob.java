package de.tuberlin.campus;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

public class BatchJob {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Total quantity of items sold per StockCode
        DataSet<Tuple3<String, String, Integer>> itemsQuantity = env
                .readCsvFile("./src/main/resources/online_retail.csv")
                .fieldDelimiter(",")
                // include fields: StockCode, Description, Quantity
                .includeFields("01110000")
                .ignoreFirstLine()
                .types(String.class, String.class, Integer.class)
                // group by field: StockCode
                .groupBy(0)
                // summ by Quantity
                .sum(2);

        // Print the result
        itemsQuantity.print();

        /* Execute program with sink to file
        itemsQuantity.sortPartition(2, Order.DESCENDING)
                .setParallelism(1)
                .writeAsCsv("./src/main/resources/quantity.csv", FileSystem.WriteMode.OVERWRITE);

        env.execute("Flink Batch Job");
        */
    }
}
