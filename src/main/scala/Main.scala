import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.classification.{LogisticRegression, BinaryLogisticRegressionSummary}
import org.apache.spark.sql.functions.rand
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.udf
import scala.util.matching.Regex
import org.apache.spark.sql.functions.{col, when}

//Don't pay attention of red messages INFO (it's not an error)
/**
  * The programm that predict if a user clicks on an or not
  */
object Main extends App {

  /*// Disable logging
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)*/

  val context = SparkSession
    .builder()
    .appName("Word count")
    .master("local")
    .getOrCreate()

  context.sparkContext.setLogLevel("WARN")

  //Put your own path to the json file
  //select your variable to add and change inside the variable columnVectorialized and dataModel at the end of the code 
  val untreatedData = context.read.json("./src/resources/data-students.json").select("appOrSite","network", "bidfloor", "label")

  val network = udf {(str: String) =>
    val fr = new Regex("208-(.*)")
    val can = new Regex("302-(.*)")
    val es = new Regex("214-(.*)")
    val tur = new Regex("286-(.*)")
    val ger = new Regex("262-(.*)")

    str match {
    case fr(x) => "France"
    case null | "Other" | "Unknown" => "Unknown"
    case can(x) => "Canada"
    case es(x) => "Espagne"
    case tur(x) => "Turquie"
    case ger(x) => "Allemagne"
    case _ => "Unknown"
    }
  }

  val bidfloor: Double => Int = x => x match {
  case x if (x >= 0 && x < 2) => 1
  case x if (x >= 2 && x < 4) => 3
  case x if (x >= 4 && x < 6) => 5
  case x if (x >= 4 && x < 6) => 7
  case x if (x >= 4 && x < 6) => 9
  case _ => 11  // catch-all
}
val groupUDF = udf(bidfloor)

val df = untreatedData.withColumn("bidfloor", when(col("bidfloor").isNull, 3).otherwise(col("bidfloor")))
.withColumn("bidfloor", groupUDF(col("bidfloor")))
.withColumn("label", when(col("label") === true, 1).otherwise(0))
.withColumn("network", network(untreatedData("network")))


  //TODO: clean interests
  //val updateInterestsArray= data.withColumn("interests", split(data("interests"), ","))
  //val updateInterestsReplaceNews= updateInterestsArray.withColumn("interests", split(data("interests"), ","))
  //updateInterestsArray.show()
  //updateInterests.select("interests").show()

  // this is used to implicitly convert an RDD to a DataFrame.
  import org.apache.spark.sql.functions._

  // Fetching column labels
  val colnames = df.schema.fields.map(col => col.name)

  // StringIndexer encodes a string column of labels to a column of label indices
  val indexers = colnames.map(
    col => new StringIndexer()
      .setInputCol(col)
      .setOutputCol(col + "Index")
      .setHandleInvalid("keep")
  )

  // Using one-hot encoding for representing states with binary values having only one digit 1
  val encoders = colnames.map(
    col => new OneHotEncoder()
      .setInputCol(col + "Index")
      .setOutputCol(col + "Encode")
  )

  val pipeline = new Pipeline().setStages(indexers ++ encoders)
  val dfEncoded = pipeline.fit(df).transform(df)

  //Add your variable inside the setInputCols by adding Encode after
  val columnVectorialized = new VectorAssembler()
<<<<<<< HEAD
    .setInputCols(Array("appOrSiteEncode","networkEncode", "bidfloorEncode"))
    .setOutputCol("features")
=======
    .setInputCols(Array("appOrSiteEncode" /*networkEncode","cityEncode","impidEncode","exchangeEncode","mediaEncode","osEncode","typeEncode"*/))
    .setOutputCol("features")

  val dataModel = columnVectorialized.transform(dfEncoded).select("appOrSiteEncode", /*networkEncode","cityEncode","impidEncode","exchangeEncode","mediaEncode","osEncode","typeEncode",*/ "label", "features")

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.1)
    .setElasticNetParam(0.1)

  def splitDf(df: DataFrame) = {
    val subsetLabelTrue = df.filter(col("label") === 1)
    val subsetLabelFalse = df.filter(col("label") === 0)
    val nTrain = df.count() * 0.7
    val nTest = df.count() * 0.3
    val training = subsetLabelTrue.orderBy(rand()).limit((nTrain * 0.5).toInt).union(subsetLabelFalse.orderBy(rand()).limit((nTrain * 0.5).toInt))
    val test = subsetLabelTrue.orderBy(rand()).limit((nTest * 0.5).toInt).union(subsetLabelFalse.orderBy(rand()).limit((nTest * 0.5).toInt))
    Array(training, test)
  }
>>>>>>> 574baaff52b24c301eb1a187a4d9902117c621e5

  val dataModel = columnVectorialized.transform(dfEncoded).select("appOrSiteEncode","networkEncode", "bidfloorEncode", "label", "features")

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0)
    .setElasticNetParam(0)
    .setFeaturesCol("features")
    .setLabelCol("label")

<<<<<<< HEAD
   val splitData = dataModel.randomSplit(Array(0.7, 0.3))
  var (trainingData, testData) = (splitData(0), splitData(1))

  trainingData = trainingData.select("features", "label")
  trainingData.groupBy("label").count.show()
  testData = testData.select("features", "label")
  testData.groupBy("label").count.show()

  // Fit the model
  val lrModel = lr.fit(trainingData)

  val predictions = lrModel.transform(testData)

=======
  val predictions = lrModel.transform(splitData(1))

>>>>>>> 574baaff52b24c301eb1a187a4d9902117c621e5
  // Print the coefficients and intercept for logistic regression
  val trainingSummary = lrModel.summary

  // Obtain the objective per iteration.
  val objectiveHistory = trainingSummary.objectiveHistory

  // Obtain the metrics useful to judge performance on test data.
  // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a
  // binary classification problem.
  val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

  // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
  val roc = binarySummary.roc
  roc.show()
  println(s"areaUnderROC: ${binarySummary.areaUnderROC}")

  println("objectiveHistory:")
  objectiveHistory.foreach(loss => println(loss))

  println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

  context.stop()
}