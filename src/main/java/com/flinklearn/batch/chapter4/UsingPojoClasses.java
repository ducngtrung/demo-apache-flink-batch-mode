package com.flinklearn.batch.chapter4;

import com.flinklearn.batch.common.Utils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.List;

/*
One of the pain points that you would have experienced is creating and using large tuples.
A better solution is to declare a POJO Java class for that structure and use it to store data in the Dataset.
 */
public class UsingPojoClasses {

    public static void main(String[] args) {

        try {

            Utils.printHeader("Starting the POJO Classes program...");

            //Get execution environment
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            /* Load the Product vendor information into a POJO Dataset.
            CSVs can be loaded as and processed as classes. */
            DataSet<ProductVendorPojo> productVendor
                    = env.readCsvFile("src/main/resources/product_vendor.csv")
                    .ignoreFirstLine()  //the first line is header
                    .pojoType(ProductVendorPojo.class, "product", "vendor");
                    /* Use the pojoType() method to map the input to the ProductVendor POJO class.
                    Specify how the columns in the csv file map to the attributes in the POJO class. */

            //Print the contents
            System.out.println("Product Vendor details loaded: ");
            productVendor.print();

            //Calculate Product counts by Vendor
            DataSet<Tuple2<String, Integer>> vendorSummary
                = productVendor
                        /* Select the Vendor and the count of 1 per record.
                        Instead of using the tuple member identifiers (f0, f1,...),
                        now we can reference attribute names of the POJO class. */
                        .map(i -> Tuple2.of(i.vendor, 1))
                        .returns(Types.TUPLE(Types.STRING, Types.INT))
                        .groupBy(0)   //Group by Vendor
                        .reduce( (summaryRow, nextRow) ->   //Reduce operation
                                new Tuple2(summaryRow.f0, summaryRow.f1 + nextRow.f1));

            //Convert Dataset to List and pretty print
            System.out.printf("\n%15s  %10s\n", "Vendor", "Products");
            System.out.println("---------------------------");
            List<Tuple2<String,Integer>> vendorList = vendorSummary.collect(); //Use collect() to convert DataSet to List
            for (Tuple2<String, Integer> vendorRecord : vendorList) {
                System.out.printf("%15s  %10s\n", vendorRecord.f0, vendorRecord.f1);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
