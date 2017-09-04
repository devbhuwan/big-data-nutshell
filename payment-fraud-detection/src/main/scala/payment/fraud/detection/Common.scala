package payment.fraud.detection

import java.util.Date

import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}

/**
  * @author Bhuwan Prasad Upadhyay
  *
  */
object Common {

  val paymentFraudDetection = "PaymentFraudDetection"

  val paymentSchema = StructType(Array(
    StructField("id", StringType, nullable = true),
    StructField("customerName", StringType, nullable = true),
    StructField("cardNo", StringType, nullable = true),
    StructField("location", StringType, nullable = true),
    StructField("amount", DoubleType, nullable = true),
    StructField("time", DateType, nullable = true)
  ))

  case class Payment(id: String, customerName: String, cardNo: String, location: String, amount: Double, time: Date)

}
