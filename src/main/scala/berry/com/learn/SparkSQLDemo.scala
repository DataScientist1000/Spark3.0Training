package berry.com.learn

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object SparkSQLDemo extends App {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .appName("SparkSQLDemo")
    .master("local[3]")
    .getOrCreate()

  val surveyDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/sample.csv")

  surveyDF.show()
//  surveyDF.createOrReplaceTempView("survey_tbl")
//  val countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")

  //scala.io.StdIn.readLine()
  spark.stop()

}
