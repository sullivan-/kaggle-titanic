import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.mllib.regression.LabeledPoint

object TrainDecisionTree extends App {

  val sparkSession = SparkSession
    .builder
    .appName("StringIndexerExample")
    .getOrCreate()

  val sparkContext = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext

  import org.apache.spark.sql.functions._
  import sqlContext.implicits._

  val data = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/discrete.csv")
    .filter("Survived is not null")
    .map(util.toPoint)
    .rdd

  // Split the data into training and test sets (30% held out for testing)
  val splits = data.randomSplit(Array(0.7, 0.3))
  val (trainingData, testData) = (splits(0), splits(1))

  // Train a DecisionTree model. Empty categoricalFeaturesInfo indicates all features are continuous.
  val numClasses = 2
  val categoricalFeaturesInfo = Map[Int, Int]()
  val impurity = args(0)       // "gini"
  val maxDepth = args(1).toInt // 5
  val maxBins  = args(2).toInt // 32

  val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
    impurity, maxDepth, maxBins)

  // Evaluate model on test instances and compute test error
  val labelAndPreds = testData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }
  val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
  println(s"Test Error = $testErr")
  println(s"Learned classification tree model:\n ${model.toDebugString}")

  // model.save(sparkContext, "decisionTreeClassificationModel")
  // val sameModel = DecisionTreeModel.load(sc, "decisionTreeClassificationModel")

}

