import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors

object util {
  
  def toPoint(row: Row) = {
    new LabeledPoint(row.getInt(1).toDouble, toVector(row))
  }

  def toVector(row: Row) = Vectors.dense(Array(
    row.getAs[Int]   (2).toDouble, // Pclass
    row.getAs[Double](3),          // Age
    row.getAs[Double](4),          // Fare
    row.getAs[Int]   (5).toDouble, // FamilySize
    row.getAs[Int]   (6),          // NumCabins
    row.getAs[Double](7),         // Sex_discrete
    row.getAs[Double](8),         // Embarked_discrete
    row.getAs[Double](9),         // Title_discrete
    row.getAs[Double](10),         // TicketCode_discrete
    row.getAs[Double](11)          // Deck_discrete
  ))

}
