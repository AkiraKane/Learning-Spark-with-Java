package project8;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class LinearMarketingVsSales {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("LinearRegressionExample")
                .master("local")
                .getOrCreate();

        String filePath = "/Users/akira/sparkwithjava/Data/marketing-vs-sales.csv";

        Dataset<Row> markVsSalesDF = spark.read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", "true")
                .load(filePath);

        // markVsSalesDF.show();

        Dataset<Row> mldf = markVsSalesDF
                .withColumnRenamed("sales", "label")
                .select("label", "marketing_spend");

        // mldf.show();

        String[] featureCols = {"marketing_spend"};

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(featureCols)
                .setOutputCol("features");

        Dataset<Row> lblFeaturesDF = assembler.transform(mldf)
                .select("label", "features");

        lblFeaturesDF.na().drop();

        // lblFeaturesDF.show();

        // we need to create a linear regression model object
        LinearRegression lr = new LinearRegression();
        LinearRegressionModel learningModel = lr.fit(lblFeaturesDF);

        System.out.println("R squared: " + learningModel.summary().r2());
        // learningModel.summary().residuals();

        learningModel.summary().predictions().show();

    }
}
