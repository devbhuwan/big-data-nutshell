package payment.fraud.detection

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{Dataset, SparkSession}
import payment.fraud.detection.Common.{Payment, paymentSchema}

/**
  * @author Bhuwan Prasad Upadhyay
  *
  */
object PaymentFraudDetectionExample {


  def main(args: Array[String]): Unit = {
    val paymentFraudDetection = "PaymentFraudDetection"
    val spark: SparkSession = SparkSession.builder().appName(paymentFraudDetection)
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._
    val train: Dataset[Payment] = spark.read.option("paymentSchema", "false")
      .schema(paymentSchema).csv("payment-fraud-detection/data/payment.csv").as[Payment]
    train.take(1)
    train.cache
    println(train.count)

    val test: Dataset[Payment] = spark.read.option("paymentSchema", "false")
      .schema(paymentSchema).csv("payment-fraud-detection/data/payment-test.csv").as[Payment]
    test.take(2)
    println(test.count)
    test.cache

    train.printSchema()
    train.show
    train.createOrReplaceTempView("payment")
    spark.catalog.cacheTable("payment")

    train.groupBy("location").count.show

    val fractions = Map("False" -> .17, "True" -> 1.0)
    //Here we're keeping all instances of the Churn=True class, but downsampling the Churn=False class to a fraction of 388/2278.
    val strain = train.stat.sampleBy("location", fractions, 36L)

    strain.groupBy("location").count.show
    val ntrain = strain.drop("state").drop("acode").drop("vplan").drop("tdcharge").drop("techarge")
    println(ntrain.count)
    ntrain.show

    val customerNameIndexer = new StringIndexer()
      .setInputCol("customerName")
      .setOutputCol("customerNameIndex")
    val locationIndexer = new StringIndexer()
      .setInputCol("location")
      .setOutputCol("locationIndex")

    val featureCols = Array("id", "customerName", "cardNo", "location", "amount", "time")

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val dTree = new DecisionTreeClassifier().setLabelCol("label")
      .setFeaturesCol("features")

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(customerNameIndexer, locationIndexer, assembler, dTree))
    // Search through decision tree's maxDepth parameter for best model
    val paramGrid = new ParamGridBuilder().addGrid(dTree.maxDepth, Array(2, 3, 4, 5, 6, 7)).build()

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")

    // Set up 3-fold cross validation
    val crossval = new CrossValidator().setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid).setNumFolds(3)

    val cvModel = crossval.fit(ntrain)

    val bestModel = cvModel.bestModel
    println("The Best Model and Parameters:\n--------------------")
    println(bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(3))
    bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
      .stages(3)
      .extractParamMap

    val treeModel = bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(3).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)

    val predictions = cvModel.transform(test)
    val accuracy = evaluator.evaluate(predictions)
    evaluator.explainParams()

    val predictionAndLabels = predictions.select("prediction", "label").rdd.map(x =>
      (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    println("area under the precision-recall curve: " + metrics.areaUnderPR)
    println("area under the receiver operating characteristic (ROC) curve : " + metrics.areaUnderROC)

    println(metrics.fMeasureByThreshold())

    val result = predictions.select("label", "prediction", "probability")
    result.show
    train.printSchema()
    train.show
  }


}
