package project7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class StreamingFileDirectoryApplication {

    public static void main(String[] args) throws StreamingQueryException{

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("StreamingFileDirectoryWordCount")
                .master("local")
                .getOrCreate();

        // Read all csv files written automatically in a directory
        StructType userSchema = new StructType().add("date", "string").add("value", "float");
        Dataset<Row> stockData = spark
                .readStream()
                .option("sep", ",")
                .schema(userSchema)  // specify schema of csv
                .csv("/Users/akira/sparkwithjava/Data/IncomingStockFiles"); // equivalent to format("csv").load("")

        Dataset<Row> resultDF = stockData.groupBy("date").agg(avg(stockData.col("value")));

        StreamingQuery query = resultDF.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();

    }


}
