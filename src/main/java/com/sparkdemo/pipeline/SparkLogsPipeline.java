package com.sparkdemo.pipeline;

import com.sparkdemo.pipeline.dto.Log;
import com.sparkdemo.pipeline.dto.Stream;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import scala.collection.Seq;

public class SparkLogsPipeline {

    public SparkLogsPipeline() {
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkPi")
                .enableHiveSupport()
                .getOrCreate();


        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());


        Dataset<Row> logs = spark.read().csv(
                "/incoming/logs/upload/tiger/*"
        );

        Dataset<Log> logDataset = logs.as(Encoders.bean(Log.class));

        logDataset.printSchema();

        logDataset.filter(
                (FilterFunction<Log>) l -> l.getType().equals("SongPlayed")
        );

        long filteredLogs = logDataset.count();
        System.out.println(String.format("There are %d filtered logs ", logDataset));
    }
}
