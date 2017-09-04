package payment.fraud.detection

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{Dataset, SparkSession}
import payment.fraud.detection.Common.{Payment, paymentFraudDetection, paymentSchema}

/**
  * @author Bhuwan Prasad Upadhyay
  *
  */
object PaymentFraudDetectionExample {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName(paymentFraudDetection)
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val train: Dataset[Payment] = spark.read.option("paymentSchema", "false")
      .schema(paymentSchema).csv("payment-fraud-detection/data/payment.csv")
      .as[Payment]

    train.take(1)
    train.cache
    println(train.count)

    train.createOrReplaceTempView("payment")
    spark.catalog.cacheTable("payment")
    train.show

    val lr = new LinearRegression();
    lr.setPredictionCol("predicated_location").setLabelCol("location").setMaxIter(100)
      .setRegParam(0.1)

    val lrPipeline = new Pipeline();
    lrPipeline.setStages(Array(vec))

  }


}
