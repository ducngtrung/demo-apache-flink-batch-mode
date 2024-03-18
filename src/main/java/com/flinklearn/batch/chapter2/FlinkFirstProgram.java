package com.flinklearn.batch.chapter2;

import com.flinklearn.batch.common.Utils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.FileSystem;

import java.util.Arrays;
import java.util.List;

/*
A simple Flink program that counts the number of elements in a collection.
Running this program inside the IDE will create an embedded Apache Flink environment.
 */
public class FlinkFirstProgram {

    public static void main(String[] args) {

        try {

            Utils.printHeader("Starting the Batch Sample program...");

            /* Get the execution environment.
            While running inside IDE, it will create an embedded environment.
            While running inside a Flink installation, it will acquire the current context. */
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            /* Create a list of products */
            List<String> products = Arrays.asList("Mouse","Keyboard","Webcam");

            /* Convert the list into a Flink DataSet */
            DataSet<String> dsProducts = env.fromCollection(products);

            /* Count the number of items in the DataSet.
            Flink uses lazy execution, so all code is executed only when an output is requested. */
            System.out.println("Total products = " + dsProducts.count());

            /* Write results to a file, overwrite if the file path already exists */
            dsProducts.writeAsText("output/tempdata.csv", FileSystem.WriteMode.OVERWRITE);

            /* Print execution plan */
            System.out.println("Execution plan:\n" + env.getExecutionPlan());

            //Explicitly call execute() to trigger the program execution
            env.execute();

        } catch (Exception e) {
            System.out.println(Arrays.toString(e.getStackTrace()));
        }

    }

}
