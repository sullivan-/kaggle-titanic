
/** takes the following two inputs:
 * 
 * - data/train.csv
 * - data/test.csv
 *
 * joins them into one data set, adding nulls for column `Survived` to the rows in `test.csv`.
 *
 * it outputs the result to directory `data/merge_out`.
 */
object Merge extends App {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.SparkContext
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.ml.feature.StringIndexer

  val sparkSession = SparkSession.builder.appName("Merge").getOrCreate()

  val sparkContext = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext

  import org.apache.spark.sql.functions._
  import sqlContext.implicits._

  val train = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/train.csv")

  val test = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/test.csv")
    .withColumn("Survived", lit(null))

  val merged = train unionByName test

  merged.coalesce(1).write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .save("data/merge_out")

}

