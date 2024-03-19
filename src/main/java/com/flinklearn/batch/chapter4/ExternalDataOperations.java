package com.flinklearn.batch.chapter4;

import com.flinklearn.batch.common.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Types;

/*
Flink typically needs to read and write data with external data sources.
This program demonstrates how Flink uses JDBC to read and write data with MySQL.
You need to create 2 tables called 'sales_orders' (with the same fields and content as the sales_orders.csv file)
and 'customer_summary' (with no data populated) in MySQL. You will use Flink to read data from the 'sales_orders' table,
perform computation and write output data to the 'customer_summary' table. The table creation scripts are available
at src/main/resources/create_mysql_tables.sql.
 */
public class ExternalDataOperations {

    public static void main(String[] args) {

        try {

            Utils.printHeader("Starting the Data Source/Sink program...");

            //Get execution environment
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            /****************************************************************************
             *                           Load data from MySQL
             ****************************************************************************/

            /* Define data types for specific columns read from the database table.
            We will read the Customer, Quantity, and Rate columns from the sales_orders table. */
            TypeInformation[] orderFieldTypes = new TypeInformation[] {
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.DOUBLE_TYPE_INFO
            };

            //Create a RowTypeInfo object based on the TypeInformation
            RowTypeInfo orderRowInfo = new RowTypeInfo(orderFieldTypes);

            //Create a JDBCInputFormat object to read data from JDBC
            JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                    .setDrivername("com.mysql.cj.jdbc.Driver")
                    .setDBUrl("jdbc:mysql://localhost:3307/flink")
                    .setUsername("root")
                    .setPassword("")
                    .setQuery("SELECT Customer, Quantity, Rate from sales_orders")
                    .setRowTypeInfo(orderRowInfo)  //Set row information (data types) for the returned values
                    .finish();

            /* Create a Dataset to retrieve data from the Data Source (returned by the JDBC connection).
            Note that the Dataset is of type Row, not Tuple. */
            DataSet<Row> orderRecords = env.createInput(jdbcInputFormat);

            Utils.printHeader("Retrieved Data from Database (top 10)" +
                    "\n(Customer, Quantity, Rate)");
            orderRecords.first(10).print();

            /****************************************************************************
             *                         Perform Data Processing
             ****************************************************************************/

            //Use Map to calculate Order Value ( Quantity * Rate )
            DataSet<Tuple2<String,Double>> customerOrderValues  //(Customer, Order Value)
                    = orderRecords
                        .map( new MapFunction<Row, Tuple2<String, Double>>() {
                            @Override
                            public Tuple2<String, Double> map(Row row) throws Exception {
                                return new Tuple2(
                                        row.getField(0), //Get Customer name
                                        (Integer)row.getField(1) * (Double)row.getField(2) //Get Order Value
                                );
                            }
                        } );

            //Summarize total Order Value by Customer
            DataSet<Tuple2<String,Double>> customerOrderSummary  //(Customer, Total Order Value)
                    = customerOrderValues
                        .groupBy(0)  //Group by Customer
                        .reduce( new ReduceFunction<Tuple2<String, Double>>() {
                            @Override
                            public Tuple2<String, Double> reduce(
                                    Tuple2<String, Double> row1,
                                    Tuple2<String, Double> row2
                            ) {
                                return new Tuple2(row1.f0, row1.f1 + row2.f1);
                            }
                        } );

            //Use Map to convert Tuple back to Row
            DataSet<Row> summaryResult
                    = customerOrderSummary
                        .map( new MapFunction<Tuple2<String, Double>, Row>() {
                            @Override
                            public Row map(Tuple2<String, Double> record) throws Exception {
                                return Row.of(record.f0,record.f1);
                                /* Create rows by using Row.of() function and passing parameters for the row columns. */
                            }
                        } );

            Utils.printHeader("Computed Customer Summary " +
                    "\n(Customer, Total Order Value)");
            summaryResult.print();

             /****************************************************************************
             *                        Write output data to MySQL
             ****************************************************************************/

            //Create a JDBCOutputFormat object to write data to JDBC
            JDBCOutputFormat jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
                            .setDrivername("com.mysql.cj.jdbc.Driver")
                            .setDBUrl("jdbc:mysql://localhost:3307/flink")
                            .setUsername("root")
                            .setPassword("")
                            .setQuery("INSERT INTO customer_summary VALUES (?,?) ")
                            .setSqlTypes(new int[] {Types.VARCHAR, Types.DOUBLE}) //Define SQL data type for each column
                            .finish();

            //Define Data Sink for the summary data
            summaryResult.output(jdbcOutputFormat);

            //Explicitly call execute() to trigger the program execution
            env.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
