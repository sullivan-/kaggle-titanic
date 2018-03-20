import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

object RunDecisionTree extends App {

  val sparkSession = SparkSession.builder.appName("RunDecisionTree").getOrCreate()
  val sparkContext = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext

  import org.apache.spark.sql.functions._
  import sqlContext.implicits._

  val data = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/discrete.csv")
    .filter("Survived is null")
    .map(row => new LabeledPoint(row.getInt(0).toDouble, util.toVector(row)))

  val model = DecisionTreeClassificationModel.load("model")

  case class P(PassengerId: Int, Survived: Int)

  val submission = model.transform(data).map({ r =>
    val id = r.getAs[Double]("label").toInt
    val prediction = r.getAs[Double]("prediction").toInt
    P(id, prediction)
  })

  submission.coalesce(1).write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .save("data/submission_out")

}

