
/** takes the following input:
 * 
 * - data/merged.csv
 *
 * and discretizes the specified columns.
 *
 * it outputs the result to data/clean_out`.
 */
object Clean extends App {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.SparkContext
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.ml.feature.StringIndexer

  val sparkSession = SparkSession.builder.appName("Clean").getOrCreate()

  val sparkContext = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext

  import org.apache.spark.sql.functions._
  import sqlContext.implicits._

  val merged = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/merged.csv")

  val titleUdf = udf { name: String =>
    val nameNoFirst = name.substring(name.indexOf(','))
    val rawTitle = nameNoFirst.substring(2, nameNoFirst.indexOf('.'))
    rawTitle match {
      case "Capt" | "Col" | "Major" => "Military"
      case "Don" | "Dona" | "Jonkheer" | "Lady" | "Sir" | "the Countess" => "Royalty"
      case "Mlle" => "Miss"
      case "Mme" | "Ms" => "Mrs"
      case _ => rawTitle
    }
  }

  val sumUdf = udf { (x: Int, y: Int) => x + y }
  val ticketCodeUdf = udf { ticket: String => ticket.filter(_.isLetter).map(_.toUpper).take(3) }
  // val ticketCodeUdf = udf { ticket: String =>
  //   val stripped = ticket.filter(_.isLetter).map(_.toLower)
  //   val ca = stripped.contains("ca")
  //   val pc = stripped.contains("pc")
  //   val sc = stripped.contains("sc") || stripped.contains("soc") || stripped.contains("soton") ||
  //   stripped.contains("stono")
  //   val code = if (pc) "PC" else if (ca) "CA" else if (sc) "SC" else "NA"
  //   code
  // }
  val deckUdf = udf { cabin: String => if (cabin == null) null else cabin.substring(0, 1) }
  val numCabins = udf { cabin: String => if (cabin == null) 0 else cabin.split(" ").length }
  val clean = merged
    .withColumn("Title", titleUdf(col("Name")))
    .withColumn("FamilySize", sumUdf(col("SibSp"), col("Parch")))
    .withColumn("TicketCode", ticketCodeUdf(col("Ticket")))
    .withColumn("Deck", deckUdf(col("Cabin")))
    .withColumn("NumCabins", numCabins(col("Cabin")))
    .drop("Name").drop("Cabin").drop("Ticket").drop("Parch").drop("SibSp")
  // TODO:
  // - try dropping "useless" columns (Name, Cabin, Ticket, Parch, SubSp)
  //   - edit Discretize & util.toVector on adding columns

  clean.coalesce(1).write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .save("data/clean_out")

}

