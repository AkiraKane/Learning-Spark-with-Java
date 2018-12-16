package project1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class Application {

    public static void main(String[] args) {

        // Create a session
        SparkSession spark = new SparkSession.Builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        // get data
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", true)
                .load("/Users/akira/sparkwithjava/project1/src/main/resources/name_and_comments.txt");


        // transformation
        // add the full name column
        df = df.withColumn("full_name", concat(df.col("last_name"),
                lit(", "),
                df.col("first_name")));

        // detect any numbers existing in the comment column
        df = df.filter(df.col("comment").rlike("\\d+")).orderBy(df.col("last_name").asc());

        String dbConnectionURL = "jdbc:postgresql://localhost/course_data";
        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "postgres");
        prop.setProperty("password", "password");

        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionURL, "project1", prop);

    }


}
