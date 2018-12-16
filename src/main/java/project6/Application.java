package project6;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;



public class Application {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("Learning Spark SQL DataFrame API")
                .master("local")
                .getOrCreate();

        String redditFile = "/Users/akira/sparkwithjava/Data/Reddit-2007-small.json";

        Dataset<Row> redditDF = spark.read().format("json")
                .option("inferSchema", "true")
                .option("header", true)
                .load(redditFile);

        redditDF.show(10);

        redditDF = redditDF.select("body");
        Dataset<String> wordsDS = redditDF.flatMap((FlatMapFunction<Row, String>)
                row -> Arrays.asList(row.toString().replace("/n", "").replace("/r", "").trim()
                        .toLowerCase().split(" ")).iterator(),
                Encoders.STRING());

        Dataset<Row> wordsDF = wordsDS.toDF();

        Dataset<Row> boringWordsDF = spark.createDataset(Arrays.asList(WordUtils.stopWords),
                Encoders.STRING()).toDF();

        // boringWordsDF.show();

        // wordsDF = wordsDF.except(boringWordsDF);  // this removes the duplicates from the end dataframe
        wordsDF = wordsDF.join(boringWordsDF, wordsDF.col("value").equalTo(boringWordsDF.col("value")),
                "leftanti");

        wordsDF = wordsDF.groupBy("value").count();
        wordsDF.orderBy(desc("count")).show();


    }
}