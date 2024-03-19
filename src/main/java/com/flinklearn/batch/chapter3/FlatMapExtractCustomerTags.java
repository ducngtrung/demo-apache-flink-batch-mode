package com.flinklearn.batch.chapter3;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;

public class FlatMapExtractCustomerTags implements FlatMapFunction<
        Tuple7<Integer, String, String, String, Integer, Double, String>, //Input Tuple
        Tuple3<Integer, String, String > //Output Tuple
        >
    {
        @Override
        public void flatMap(
                Tuple7<Integer, String, String, String, Integer, Double, String> order,
                Collector<Tuple3<Integer, String, String>> tupleCollector
        ) {

            // Extract relevant columns from the master tuple
            Integer id = order.f0;
            String customer = order.f1;
            String tags = order.f6;

            /* Split the tags string. For each tag, collect the customer
            and the tag as a separate record */
            for (String tag : tags.split(":")) {
                tupleCollector.collect(new Tuple3(id, customer, tag)); //use the Collector object to collect multiple tuples
            }

        }
    }
