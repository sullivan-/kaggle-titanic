import org.apache.spark.sql.Row
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

object util {
  
  def toPoint(row: Row) = {
    val a = Array(
      row.getAs[Int](2).toDouble, // Pclass
      row.getAs[Double](3),       // Age
      row.getAs[Int](4).toDouble, // SibSp
      row.getAs[Int](5).toDouble, // Parch
      row.getAs[Double](6),       // Fare
      row.getAs[Double](7),       // Name_discrete
      row.getAs[Double](8),       // Sex_discrete
      row.getAs[Double](9),       // Ticket_discrete
      row.getAs[Double](10),      // Cabin_discrete
      row.getAs[Double](11))      // Embarked_discrete
    new LabeledPoint(row.getInt(1).toDouble, Vectors.dense(a))
  }

}
