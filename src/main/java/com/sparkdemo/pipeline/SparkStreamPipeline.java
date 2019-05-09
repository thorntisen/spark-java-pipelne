package com.sparkdemo.pipeline;

import com.sparkdemo.pipeline.dto.Stream;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
//import com.hortonworks.hwc.HiveWarehouseSession;
//import static com.hortonworks.hwc.HiveWarehouseSession.*;

public class SparkStreamPipeline {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkPi")
                .enableHiveSupport()
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        Dataset<Row> streamRows = spark.read().table("tiger.stream");

        Dataset<Stream> streamDataset = streamRows.as(Encoders.bean(Stream.class));

        streamDataset.filter(
                (FilterFunction<Stream>) s -> s.getDuration() < 1200 && s.getDuration() > 0
        );

//        HiveWarehouseSession hive = HiveWarehouseSession.session(spark).build();
//
//        streamDataset
//                .write()
//                .format(HIVE_WAREHOUSE_CONNECTOR)
//                .mode(SaveMode.Overwrite)
//                .saveAsTable("tiger.stream_from_java");

        int records = (int)streamDataset.count();

        System.out.println(String.format("tiger.stream has %d records ", records));
    }
}
