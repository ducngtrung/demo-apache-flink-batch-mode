package com.flinklearn.batch.chapter3;

import com.flinklearn.batch.common.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

/*
A Flink program that demonstrates Dataset API record-level operations (map, flatMap, filter)
and aggregation operations (aggregate, groupBy, reduce).
Running this program inside the IDE will create an embedded Apache Flink environment.
 */

public class BasicTransformations {

    public static void main(String[] args) {

        try {

            Utils.printHeader("Starting the Basic Transformations program...");

            // Get execution environment
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            /****************************************************************************
             *                       Read CSV file into a DataSet
             ****************************************************************************/

            /* Make sure that the tuple with the correct number of elements is chosen
            to match the number of columns in the CSV file. */
            DataSet<Tuple7<Integer, String, String, String,
                                        Integer, Double, String>> rawOrders
                    = env.readCsvFile("src/main/resources/sales_orders.csv")
                        .ignoreFirstLine()  //the first line is header
                        .parseQuotedStrings('\"')
                        .types(Integer.class, String.class, String.class, String.class,
                                Integer.class, Double.class, String.class);
                        /* Recall that Flink DataSets are strongly typed and need a schema for data stored in them. */

            Utils.printHeader("Raw orders read from CSV file (top 5)");
            // Call the print() method to trigger Flink execution. Here we print the first 5 tuples in the dataset.
            rawOrders.first(5).print();

            /****************************************************************************
             *             Use Map to calculate Order Value for each record
             ****************************************************************************/

            DataSet<Tuple8<Integer, String, String, String,
                                Integer, Double, String, Double>> computedOrders
                    = rawOrders.map( new MapComputeTotalOrderValue() );

            /* While executing map functions, Flink optimizes for parallel execution by partitioning the input dataset
            and sending the partitions to different task slots.
            The task slots then execute the map function in parallel on these partitions. */
            Utils.printHeader("Orders with calculated Order Value (top 5)");
            computedOrders.first(5).print();

            /****************************************************************************
             *      Use Flat Map to extracts Tags by Customer into separate records
             ****************************************************************************/

            DataSet<Tuple3<Integer, String, String>> customerTags
                    = rawOrders.flatMap( new FlatMapExtractCustomerTags() );

            Utils.printHeader("Customer and Tags extracted as separate records (top 10)");
            customerTags.first(10).print();

            /****************************************************************************
             *                Filter Orders for the first 10 days of November
             ****************************************************************************/

            DataSet<Tuple8<Integer, String, String, String,
                                Integer, Double, String, Double>> filteredOrders
                        = computedOrders.filter( new FilterOrdersByDate() );

            Utils.printHeader("Orders filtered for the first 10 days of November");
            filteredOrders.print();
            System.out.println("\nTotal orders in the first 10 days of November = " + filteredOrders.count());

            /****************************************************************************
             *                        Aggregate across all orders
             ****************************************************************************/

            // Use the project() method to select a subset of columns (Quantity, Order Value) to work on aggregation
            DataSet<Tuple2<Integer, Double>> orderColumns
                    = computedOrders.project(4,7);

            // Find Total Order Quantity and Total Order Value
            DataSet totalOrders
                    = orderColumns
                             .aggregate(SUM,0) //Total Order Quantity
                             .and(SUM,1); //Total Order Value
                             /* The and() operator allows multiple aggregations in a single line */

            /* Extract a summary row tuple by converting the above DataSet to a List (using the collect() method)
            and fetching the first element from the List */
            Tuple2<Integer, Double> summaryRow
                    = (Tuple2<Integer, Double>) totalOrders.collect().get(0);

            Utils.printHeader("Aggregated order data");
            System.out.println("Total Order Value = " + summaryRow.f1
                                + "\nAverage Order Value = " + summaryRow.f1 / computedOrders.count()
                                + "\nTotal Order Quantity = " + summaryRow.f0
                                + "\nAverage Order Quantity = " + summaryRow.f0 * 1.0 / computedOrders.count());

            /****************************************************************************
             *                Group by Product and use Reduce to summarize
             ****************************************************************************/

            DataSet<Tuple3<String,Integer,Double>> productOrderSummary
                    = computedOrders
                            /* Select a subset of columns:
                                f2 ~ Product,
                                f7 ~ Order Value,
                                each order line item is counted as 1 */
                            .map(i -> Tuple3.of(i.f2, 1, i.f7)) //This is an inline map function
                            .returns(Types.TUPLE(Types.STRING, Types.INT, Types.DOUBLE)) //Set return types
                            .groupBy(0)  //Group by Product column
                            .reduce( new ReduceProductwiseSummary() );  //Summarize by Product

            Utils.printHeader("Order summary grouped by Product " +
                    "\n(the mid column is number of order line items, not order quantity)");
            productOrderSummary.print();

            // Calculate average Order Value for each product using an inline map function
            System.out.println("\nAverage Order Value by Product:");
            productOrderSummary
                    // This is an inline anonymous map function
                    .map( new MapFunction<Tuple3<String, Integer, Double>, Tuple2<String, Double>>() {
                            @Override
                            public Tuple2<String, Double> //(Product, Average Order Value)
                                map(Tuple3<String, Integer, Double> summary) {
                                    return new Tuple2(
                                            summary.f0, //Product name
                                            summary.f2 / summary.f1 //Average Order Value by Product
                                    );
                            }
                    } )
                    .print();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
