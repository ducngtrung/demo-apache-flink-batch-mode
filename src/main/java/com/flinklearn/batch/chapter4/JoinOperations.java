package com.flinklearn.batch.chapter4;

import com.flinklearn.batch.common.Utils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

/*
Join operations allow merging of two Datasets based on a common set of columns.
They are useful for merging data from different data sources.
 */

public class JoinOperations {

    public static void main(String[] args) {

        try {

            Utils.printHeader("Starting the Dataset Join program...");

            //Get execution environment
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            /****************************************************************************
             *                            Load the Datasets
             ****************************************************************************/

            //Load the sales_orders.csv file into a Tuple Dataset
            DataSet<Tuple7<Integer, String, String, String,
                            Integer, Double, String>> rawOrders
                    = env.readCsvFile("src/main/resources/sales_orders.csv")
                        .ignoreFirstLine()
                        .parseQuotedStrings('\"')
                        .types(Integer.class, String.class, String.class, String.class,
                                Integer.class, Double.class, String.class);

            //Load the product_vendor.csv file into a POJO Dataset
            DataSet<ProductVendorPojo> productVendor
                    = env.readCsvFile("src/main/resources/product_vendor.csv")
                        .ignoreFirstLine()
                        .pojoType(ProductVendorPojo.class, "product", "vendor");

            /****************************************************************************
             *                            Join the Datasets
             ****************************************************************************/

            DataSet<Tuple2<String, Integer>> vendorOrders  //(Vendor, Order Quantity)
                    = rawOrders
                            .join(productVendor)     //Specify the DataSet to which rawOrders has to be joined
                            .where(2)            //Join field from the rawOrders Dataset (the Product field)
                            .equalTo("product")  //Join field from the productVendor Dataset
                            .with(new OrderVendorJoinSelector()); //Specify fields to be included in the joined Dataset

            //Print total Order Quantity by Vendor
            System.out.println("\nTotal Order Quantity by Vendor: " +
                    "\n(the total number of items ordered for the products supplied by each vendor)\n");
            vendorOrders
                    .groupBy(0) //Group by Vendor
                    .sum(1)  //Total Order Quantity
                    /* This sum() function can be used as an alternative for aggregate(SUM, fieldId) */
                    .print();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
