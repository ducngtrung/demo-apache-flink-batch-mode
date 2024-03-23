package com.flinklearn.batch.chapter3;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;

public class MapComputeTotalOrderValue implements MapFunction<
        Tuple7<Integer, String, String, String, Integer, Double, String>, //Input Tuple
        Tuple8<Integer, String, String, String, Integer, Double, String, Double> //Output Tuple
        >
    {
        //Takes a Tuple7 as input and returns a Tuple8 as output
        @Override
        public Tuple8<Integer,String, String,
                            String, Integer, Double, String, Double>
                    map(Tuple7<Integer,String, String, //implement the map() function required by the MapFunction interface
                            String, Integer, Double, String> order) {

            return new Tuple8(order.f0, order.f1, order.f2, order.f3, order.f4, order.f5, order.f6,
                                (order.f4 * order.f5) //Compute the total order value
            );

        }
    }
