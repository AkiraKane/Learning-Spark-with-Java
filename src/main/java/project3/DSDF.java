package project3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class DSDF {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("DS v.s. DF")
                .master("local")
                .getOrCreate();

        String[] stringList = new String[] {"Banana", "Car", "Glass", "Banana", "Computer", "Car"};
        List<String> data = Arrays.asList(stringList);

        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

        ds.show(10);
        ds.printSchema();

        Dataset<Row> df2 = ds.toDF();  // Convert dataset to dataframe

        ds = df2.as(Encoders.STRING()); // Convert datframe back to dataset


        Dataset<Row> df = ds.groupBy("value").count();
        df.show(10);

    }
}
