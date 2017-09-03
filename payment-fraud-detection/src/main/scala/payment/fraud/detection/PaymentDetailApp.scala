package payment.fraud.detection

import org.apache.spark.SparkContext

/**
  * @author Bhuwan Prasad Upadhyay
  *
  */
object PaymentDetailApp {

  def main(args: Array[String]) {

    val sc = new SparkContext("local[2]", "PaymentDetails App")

    // we take the raw data in CSV format and convert it into a set of records of the form (user, product, price)
    val data = sc.textFile("payment-fraud-detection/data/payment.csv")
      .map(_.replaceAll("\"", ""))
      .map(line => line.split(","))
      .map(paymentRecord => (paymentRecord(0), paymentRecord(1), paymentRecord(2),  paymentRecord(3), paymentRecord(4), paymentRecord(5)))

    // let's count the number of purchases
    val numPayments = data.count()

    // let's count how many unique customers made payment
    val uniqueCustomers = data.map { case (id, customerName, cardNo, location, amount, time) => customerName }.distinct().count()

    // let's sum up our total paid amount
    val totalPaidAmount = data.map { case (id, customerName, cardNo, location, amount, time) => amount.toDouble }.sum()

    // let's find our most popular product
    val productsByPopularity = data
      .map { case (id, customerName, cardNo, location, amount, time) => (location, 1) }
      .reduceByKey(_ + _)
      .collect()
      .sortBy(-_._2)
    val mostPopular = productsByPopularity(0)

    // finally, print everything out
    println("Total payments: " + numPayments)
    println("Unique customers: " + uniqueCustomers)
    println("Total paid amount: " + totalPaidAmount)
    println("Most frequently payment paid from %s with %d amount.".format(mostPopular._1, mostPopular._2))

    sc.stop()
  }


}
