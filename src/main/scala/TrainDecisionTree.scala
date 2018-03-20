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

object TrainDecisionTree extends App {

  val sparkSession = SparkSession.builder.appName("TrainDecisionTree").getOrCreate()
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

  val classifier = new DecisionTreeClassifier
  val paramGrid = new ParamGridBuilder()
    .addGrid(classifier.impurity, Array("entropy", "gini")) // chooses gini
    .addGrid(classifier.maxBins, Array(4, 6, 7, 8, 9, 10, 12, 16, 32)) // chooses 8
    .addGrid(classifier.maxDepth, Array(2, 3, 4, 5)) // chooses 3
    .build()

  val cv = new CrossValidator()
    .setEstimator(classifier)
    .setEvaluator(new BinaryClassificationEvaluator)
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(4)
    .setParallelism(2)

  val cvModel = cv.fit(data)
  val model = cvModel.bestModel.asInstanceOf[DecisionTreeClassificationModel]
  //println(s"""PARAM ${model.params.toSeq.map(model.explainParam).mkString("\nPARAM ")}""")
  //println(s"Learned classification tree model:\n ${model.toDebugString}")

  // sanity check that the cv model is reasonable
  val splits = data.randomSplit(Array(0.7, 0.3))
  val (trainingData, testData) = (splits(0), splits(1))
  val testErr = model.transform(testData).filter({ r =>
    r.getAs[Double]("label") != r.getAs[Double]("prediction")
  }).count.toDouble / testData.count
  //println(s"Test Error = $testErr")

  model.save("model")
  // val sameModel = DecisionTreeModel.load(sc, "model")

}

