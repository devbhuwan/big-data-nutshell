package payment.fraud.detection

import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{StructField, StructType}

/**
  * @author Bhuwan Prasad Upadhyay
  *
  */
object Common {

  val paymentSchema = StructType(Array(
    StructField("id", StringType, nullable = true),
    StructField("customerName", StringType, nullable = true),
    StructField("cardNo", StringType, nullable = true),
    StructField("location", StringType, nullable = true),
    StructField("amount", StringType, nullable = true),
    StructField("time", StringType, nullable = true)
  ))

  case class Payment(id: String, customerName: String, cardNo: String, location: String, amount: String, time: String)

}
