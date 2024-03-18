package com.flinklearn.batch.chapter4;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;

public class OrderVendorJoinSelector implements JoinFunction<
        Tuple7<Integer,String,String,String,Integer,Double,String>,  //Input 1
        ProductVendorPojo,  //Input 2
        Tuple2<String,Integer>  //Output
    > {
        @Override
        public Tuple2<String, Integer>  //Output (Vendor, Order Quantity)
            join(
                Tuple7<Integer,String,String,String,Integer,Double,String> order,  //Input 1
                ProductVendorPojo product  //Input 2
            ) {
            //Return Vendor and Order Quantity
            return new Tuple2(product.getVendor(), order.f4);
        }
    }
