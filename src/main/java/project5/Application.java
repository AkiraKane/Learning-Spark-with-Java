package project5;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Application {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("Learning Spark SQL Dataframe API")
                .master("local")
                .getOrCreate();

        String studentsFile = "/Users/akira/sparkwithjava/project5/src/main/resources/students.csv";

        Dataset<Row> studentsDF  = spark.read().format("csv")
                .option("inferSchema", "true")  // make sure to use the string version of true
                .option("header", true)
                .load(studentsFile);

        String gradChartFile = "/Users/akira/sparkwithjava/project5/src/main/resources/grade_chart.csv";

        Dataset<Row> gradesDF = spark.read().format("csv")
                .option("inferSchema", "true")  // make sure to use the string version of true
                .option("header", true)
                .load(gradChartFile);

        // headers are case insensitive
        studentsDF.join(gradesDF, studentsDF.col("GPA").equalTo(gradesDF.col("gpa")))
                .select(studentsDF.col("student_name"),
                        studentsDF.col("favorite_book_title"),
                        gradesDF.col("letter_grade"))
                .show();


        studentsDF.join(gradesDF, studentsDF.col("GPA").equalTo(gradesDF.col("gpa")))
                .filter(gradesDF.col("gpa").between(2, 3.5))
                .select("student_name",
                        "favorite_book_title",
                        "letter_grade")
                .show();

        Dataset<Row> filteredDF = studentsDF.join(gradesDF, studentsDF.col("GPA").equalTo(gradesDF.col("gpa")))
                .where(gradesDF.col("gpa").gt(3.0).and(gradesDF.col("gpa").lt(4.5))
                                                            .or(gradesDF.col("gpa").equalTo(1.0)))
                .select(col("student_name"),
                       col( "favorite_book_title"),
                        col("letter_grade"));

        filteredDF.show();


    }
}
