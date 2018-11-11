import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.classification.{LogisticRegression, BinaryLogisticRegressionSummary}
import org.apache.spark.sql.functions.rand
import org.apache.log4j.{Level, Logger}

//Don't pay attention of red messages INFO (it's not an error)
/**
  * The programm that predict if a user clicks on an or not
  */
object Main extends App {

  // Disable logging
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val context = SparkSession
    .builder()
    .appName("Word count")
    .master("local")
    .getOrCreate()

  context.sparkContext.setLogLevel("WARN")

  //Put your own path to the json file
  //select your variable to add and change inside the variable columnVectorialized and dataModel at the end of the code 
  val untreatedData = context.read.json("./src/resources/data-students.json").select("appOrSite" /*"network","city","impid","exchange","media","os","type"*/ , "label")

  //TODO: clean interests
  //val updateInterestsArray= data.withColumn("interests", split(data("interests"), ","))
  //val updateInterestsReplaceNews= updateInterestsArray.withColumn("interests", split(data("interests"), ","))
  //updateInterestsArray.show()
  //updateInterests.select("interests").show()

  // this is used to implicitly convert an RDD to a DataFrame.
  import org.apache.spark.sql.functions._

  // Creating dataframe while converting label from boolean to binary
  val df = untreatedData.withColumn("label", when(col("label") === true, 1).otherwise(0))

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

  val splitData = splitDf(dataModel)

  // Fit the model
  val lrModel = lr.fit(splitData(0))

  val predictions = lrModel.transform(splitData(1))

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
