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
import java.util.stream.Stream;

public class StreamingSocketApplication {

    public static void main(String[] args) throws StreamingQueryException{

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        // First start a socket connection at 9999 using this: nc -lk 9999
        SparkSession spark = SparkSession.builder()
                .appName("StreamingSocketWordCout")
                .master("local")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from onnection to localhost:9999
        Dataset<Row> lines = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", "9999")
                .load();

        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) row -> Arrays.asList(row.split(" ")).iterator(), Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode("update")
                .format("console")
                .start();

        query.awaitTermination();


    }
}
