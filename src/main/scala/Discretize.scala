
/** takes the following input:
 * 
 * - data/clean.csv
 *
 * and discretizes the specified columns.
 *
 * it outputs the result to directory `data/discrete_out`.
 */
object Discretize extends App {

  val columnsToDiscretize = Seq("Name", "Sex", "Ticket", "Cabin", "Embarked")

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.SparkContext
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.ml.feature.StringIndexer

  val sparkSession = SparkSession
    .builder
    .appName("StringIndexerExample")
    .getOrCreate()

  val sparkContext = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext

  import org.apache.spark.sql.functions._
  import sqlContext.implicits._

  val clean = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/clean.csv")

  val discrete = columnsToDiscretize.foldLeft(clean) { case (df, column) =>
    val indexer = new StringIndexer()
      .setInputCol(column)
      .setOutputCol(s"${column}_discrete")
      .setHandleInvalid("keep")
    indexer.fit(df).transform(df).drop(column)
  }

  discrete.coalesce(1).write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .save("data/discrete_out")

}

