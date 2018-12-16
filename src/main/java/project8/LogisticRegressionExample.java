package project8;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LogisticRegressionExample {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("LogisticRegressionExample")
                .master("local")
                .getOrCreate();

        String filePath = "/Users/akira/sparkwithjava/Data/cryotherapy.csv";

        Dataset<Row> treatmentDF = spark.read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", "true")
                .load(filePath);

        treatmentDF.show();

        Dataset<Row> lalFeatureDF = treatmentDF.withColumnRenamed("Result_of_Treatment", "label")
                .select("label", "sex", "age", "Time", "Number_of_Warts", "Type", "Area");

        lalFeatureDF.na().drop();

        StringIndexer genderIndexer = new StringIndexer()
                .setInputCol("sex")
                .setOutputCol("sexIndex");

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"sexIndex", "age", "Time", "Number_of_Warts", "Type", "Area"})
                .setOutputCol("features");

        Dataset<Row>[] splitData = lalFeatureDF.randomSplit(new double[] {0.7, 0.3});
        Dataset<Row> trainingDF = splitData[0];
        Dataset<Row> testingDF = splitData[1];

        LogisticRegression logreg = new LogisticRegression();

        Pipeline pl = new Pipeline();

        pl.setStages(new PipelineStage[]{genderIndexer, assembler, logreg});

        PipelineModel model = pl.fit(trainingDF);
        Dataset<Row> result = model.transform(testingDF);

        result.show();

    }
}
