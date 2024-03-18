package com.flinklearn.batch.chapter3;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class ReduceProductwiseSummary implements ReduceFunction<Tuple3<String, Integer, Double>>
    {
        @Override
        public Tuple3<String, Integer, Double> reduce(
                Tuple3<String, Integer, Double> record1, //the current (or summary) record
                Tuple3<String, Integer, Double> record2  //the next record
        ) {
        /* A reduce function works in an iterative manner, accumulating each record with a running summary record.
        As this function is called iteratively for each record in the Dataset, the summary calculated in the previous
        iteration is provided as the first input in the next iteration. The previous step (groupBy) already group all
        records for a given product name so that they are processed together.
         */
            return new Tuple3(record1.f0, //the Product name
                                record1.f1 + record2.f1, //calculate Total Order Line Items
                                record1.f2 + record2.f2  //calculate Total Order Value
            );
        }
    }
