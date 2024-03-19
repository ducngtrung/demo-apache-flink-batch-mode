package com.flinklearn.batch.chapter4;

import com.flinklearn.batch.common.Utils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

/*
When map operations happen in parallel, the data is partitioned and distributed to multiple task managers and task slots.
In some use cases, there is a need for using a small data set for lookups and references. Using joins is an expensive
operation, a better idea is to have a copy of that lookup data in each task slot, so the tasks can access them locally
which makes it very fast. A broadcast variable is sent to all tasks slots where the corresponding map process is running,
so a local copy is available for initial lookup.
This program demonstrates using broadcast variables. We will create a map of discount rate for each product and then
use it to compute discounted order value for each order in the sales_orders Dataset.
 */
public class BroadcastOperations {

    public static void main(String[] args) {

        try {

            Utils.printHeader("Starting the Broadcast Operations program...");

            //Get execution environment
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            /****************************************************************************
             *                       Set up the Broadcast Variable
             ****************************************************************************/

            //Create a map of Discount Rates by Product
            Map<String,Double> productDiscounts = new HashMap<String,Double>();
            productDiscounts.put("Mouse", 0.05);
            productDiscounts.put("Keyboard", 0.10);
            productDiscounts.put("Webcam", 0.075);
            productDiscounts.put("Headset", 0.10);

            //Create a Dataset from the productDiscounts map. This is the variable to be broadcasted.
            DataSet<Map<String,Double>> dsDiscounts = env.fromElements(productDiscounts);
            System.out.println("\nDiscount Rates by Product:");
            dsDiscounts.print();

            /****************************************************************************
             *                  Read Orders and apply discounts by Order
             ****************************************************************************/

            //Read raw order data from the sales_orders.csv file
            DataSet<Tuple7<Integer, String, String, String,
                            Integer, Double, String>> rawOrders
                    = env.readCsvFile("src/main/resources/sales_orders.csv")
                        .ignoreFirstLine()
                        .parseQuotedStrings('\"')
                        .types(Integer.class, String.class, String.class, String.class,
                                Integer.class, Double.class, String.class);

            //Calculate Discounted Net Order Values
            DataSet<Tuple2<Integer,Double>> orderNetValues
                    = rawOrders
                        //Define a RichMap function that takes a broadcast variable
                        .map( new RichMapFunction<
                                Tuple7<Integer, String, String, String, Integer, Double, String>,  //Input
                                Tuple2<Integer, Double>  //Output (Order ID, Discounted Net Order Value)
                                >() {

                            //Instance variable to hold the discounts map
                            private Map<String,Double> productDiscounts;

                            //Additional function where broadcast variable can be read
                            @Override
                            public void open(Configuration params) {
                                /* Read the broadcast variable named bcDiscounts using getBroadcastVariable() function
                                from the runtime context, and assign it to the productDiscounts instance variable */
                                this.productDiscounts
                                        = (Map<String,Double>)
                                            this.getRuntimeContext()
                                                .getBroadcastVariable("bcDiscounts").get(0);
                            }

                            @Override
                            public Tuple2<Integer, Double> map(
                                    Tuple7<Integer, String, String, String, Integer, Double, String> order  //Input
                            ) {
                                //Discounted net order value = Quantity * Rate * ( 1 - Discount Rate )
                                Double netRate = order.f4 * order.f5
                                                    * ( 1 - productDiscounts.get(order.f2) );
                                return new Tuple2(order.f0, netRate);
                            }

                        } )
                        /* Pass dsDiscounts DataSet as broadcast variable to the runtime context
                        using withBroadcastSet() function */
                        .withBroadcastSet(dsDiscounts,"bcDiscounts");

            Utils.printHeader("Net Order Values by Order ID (top 10)");
            orderNetValues.first(10).print();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
