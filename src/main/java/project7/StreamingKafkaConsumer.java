package project7;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.util.Arrays;

public class StreamingKafkaConsumer {

    public static void main(String[] args) throws StreamingQueryException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("StreamingKafkaConsumer")
                .master("local")
                .getOrCreate();

        // Kafka Consumer
        Dataset<Row> messagesDF = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test")
                .load()
                .selectExpr("CAST(value AS STRING)");

        // messagesDF.show()   // cannot do this when streaming
        Dataset<String> words = messagesDF
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>)
                        row -> Arrays.asList(row.split(" ")).iterator(), Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query  = wordCounts
                .writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();

    }


}
