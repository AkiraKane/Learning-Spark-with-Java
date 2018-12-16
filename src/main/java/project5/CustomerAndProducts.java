package project5;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.util.List;

public class CustomerAndProducts {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("Learning Spark SQL Dataframe API")
                .master("local")
                .getOrCreate();

        String customers_file = "/Users/akira/sparkwithjava/project5/src/main/resources/customers.csv";

        Dataset<Row> customersDf = spark.read().format("csv")
                .option("inferSchema", "true") // Make sure to use string version of true
                .option("header", true)
                .load(customers_file);

        String products_file = "/Users/akira/sparkwithjava/project5/src/main/resources/products.csv";

        Dataset<Row> productsDf = spark.read().format("csv")
                .option("inferSchema", "true") // Make sure to use string version of true
                .option("header", true)
                .load(products_file);

        String purchases_file = "/Users/akira/sparkwithjava/project5/src/main/resources/purchases.csv";


        Dataset<Row> purchasesDf = spark.read().format("csv")
                .option("inferSchema", "true") // Make sure to use string version of true
                .option("header", true)
                .load(purchases_file);

        System.out.println(" Loaded all files into Dataframes ");
        System.out.println("----------------------------------");

        Dataset<Row> joinedData = customersDf.join(purchasesDf, customersDf.col("customer_id")
                .equalTo(purchasesDf.col("customer_id")))
                .join(productsDf, purchasesDf.col("product_id").equalTo(productsDf.col("product_id")))
                .drop("favorite_website").drop(purchasesDf.col("customer_id"))
                .drop(purchasesDf.col("product_id")).drop("product_id");

        Dataset<Row> aggDF = joinedData.groupBy("first_name", "product_name").agg(
                count("product_name").as("number_of_purchases"),
                sum("product_price").as("total_spent"),
                max("product_price").as("most_expensive_purchase")
        );

        aggDF = aggDF.drop("number_of_purchases").drop("most_expensive_purchase");

        Dataset<Row> initialDF = aggDF;

        for(int i=0; i< 10; i++){
            aggDF =aggDF.union(initialDF);
        }

        aggDF.show(100);

        joinedData.collectAsList();

    }
}
