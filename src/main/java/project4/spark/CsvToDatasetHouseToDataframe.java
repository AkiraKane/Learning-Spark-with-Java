package project4.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import project4.mappers.HouseMapper;
import project4.pojos.House;

import static org.apache.spark.sql.functions.*;


public class CsvToDatasetHouseToDataframe {

    public void start(){

        SparkSession spark = SparkSession.builder()
                .appName("CSV to dataframe to Dataset<House> and back")
                .master("local")
                .getOrCreate();

        String fileName = "/Users/akira/sparkwithjava/project4/src/main/resources/houses.csv";

        Dataset<Row> df = spark.read().format("csv")
                .option("inferSchema", "true")  // make sure to use the string version of true
                .option("header", true)
                .option("sep", ";")
                .load(fileName);

        System.out.println("House ingested in a dataframe: ");
        df.show(5);
        df.printSchema();

        Dataset<House> houseDS = df.map(new HouseMapper(), Encoders.bean(House.class));

        System.out.println("House ingested in a dataset: ");
        houseDS.show(5);
        houseDS.printSchema();

        Dataset<Row> df2 = houseDS.toDF();
        df2 = df2.withColumn("formatedDate", concat(df2.col("vacantBy.date"), lit("_"), df2.col("vacantBy.year")));
        df2.show();

    }
}
