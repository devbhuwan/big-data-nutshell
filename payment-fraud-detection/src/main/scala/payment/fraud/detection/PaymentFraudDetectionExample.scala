package payment.fraud.detection

import java.util.Date

import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @author Bhuwan Prasad Upadhyay
  *
  */
object PaymentFraudDetectionExample {

  val schema = StructType(Array(
    StructField("id", StringType, nullable = true),
    StructField("customerName", StringType, nullable = true),
    StructField("cardNo", StringType, nullable = true),
    StructField("location", StringType, nullable = true),
    StructField("amount", StringType, nullable = true),
    StructField("time", DateType, nullable = true)
  ))

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("PaymentFraudDetection")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._
    val train: Dataset[Payment] = spark.read.option("inferSchema", "false")
      .schema(schema).csv("payment-fraud-detection/data/payment.csv").as[Payment]
    train.take(1)
    train.cache
    println(train.count)

  }

  case class Payment(id: String, customerName: String, cardNo: String, location: String, amount: BigDecimal, time: Date)

}
