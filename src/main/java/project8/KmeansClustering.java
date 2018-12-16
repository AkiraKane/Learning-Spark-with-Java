package project8;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sun.tools.asm.Assembler;

public class KmeansClustering {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("Kmeans Clustering")
                .master("local")
                .getOrCreate();

        String filePath = "/Users/akira/sparkwithjava/Data/Wholesale-customers-data.csv";

        Dataset<Row> wholesaleDF = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(filePath);

        // wholesaleDF.show();

        Dataset<Row> featuresDF = wholesaleDF.select("Channel", "Fresh", "Milk", "Grocery", "Frozen",
                "Detergents_Paper", "Delicassen");

        VectorAssembler assembler = new VectorAssembler();
        assembler.setInputCols(new String[] {"Channel", "Fresh", "Milk", "Grocery", "Frozen",
                "Detergents_Paper", "Delicassen"})
                .setOutputCol("features");

        Dataset<Row> trainData = assembler.transform(featuresDF).select("features");

        KMeans kmeans = new KMeans().setK(3);
        KMeansModel model =  kmeans.fit(trainData);

        System.out.println(model.computeCost(trainData));
        model.summary().predictions().show();



    }


}
