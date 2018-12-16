package project4.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ArrayToDataset {

    public void start(){

        SparkSession spark = SparkSession.builder()
                .appName("Array To Dataset<String>")
                .master("local")
                .getOrCreate();


        String[] stringList = new String[]{"Banana", "Car", "Glass", "Banana", "Computer", "Car"};
        List<String> data = Arrays.asList(stringList);

        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

       // ds = ds.map(new StringMapper(), Encoders.STRING());

        ds = ds.map((MapFunction<String, String>) row -> "word: " + row, Encoders.STRING());
        ds.show();

        // String stringValue = ds.reduce(new StringReducer());

        String stringValue = ds.reduce((ReduceFunction<String>) (r1, r2) -> r1 + r2);
        System.out.println(stringValue);
    }


    // Serializable class should be static
    static class StringMapper implements MapFunction<String, String>, Serializable {

        @Override
        public String call(String s) throws Exception {
            return "word: " + s;
        }
    }

    static class StringReducer implements ReduceFunction<String>, Serializable{

        @Override
        public String call(String t1, String t2) throws Exception {
            return t1 + t2;
        }
    }
}
