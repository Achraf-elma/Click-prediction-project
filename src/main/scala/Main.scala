import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
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
  val untreatedData = context.read.json("./src/resources/data-students.json").select("appOrSite","network", "timestamp", "user", "interests", "label")

  val df = untreatedData.withColumn("label", when(col("label") === true, 1).otherwise(0))
  .withColumn("network", Cleaner.udf_clean_network(untreatedData("network")))
  .withColumn("timestamp", Cleaner.udf_clean_timestamp(untreatedData("timestamp")))


  val cleanData = Cleaner.handleInterest(df)





  // this is used to implicitly convert an RDD to a DataFrame.

  // Fetching column labels
  val colnames = cleanData.schema.fields.map(col => col.name)

  // StringIndexer encodes a string column of labels to a column of label indices
  val indexers = colnames.map(
    col => new StringIndexer()
      .setInputCol(col)
      .setOutputCol(col + "Index")
      .setHandleInvalid("skip")
  )

  // Using one-hot encoding for representing states with binary values having only one digit 1
  val encoders = colnames.map(
    col => new OneHotEncoderEstimator()
      .setInputCols(Array(col + "Index"))
      .setOutputCols(Array(col + "Encode"))
  )

  val pipeline = new Pipeline().setStages(indexers ++ encoders)
  val dfEncoded = pipeline.fit(cleanData).transform(cleanData)

  println("pipeline done")

  val dfWithoutLabel = cleanData.drop("label")
  val colnamesWithoutLabel = dfWithoutLabel.schema.fields.map(col => col.name)
  val renamedEncoded = colnamesWithoutLabel.map(col => col + "Encode")

  

  //Add your variable inside the setInputCols by adding Encode after
  val columnVectorialized = new VectorAssembler()
    .setInputCols(renamedEncoded)
    .setOutputCol("features")

  val dataModel = columnVectorialized.transform(dfEncoded).select((renamedEncoded :+ "label" :+ "features").mkString(","))

  println("Vector assembler done")

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
    .setFeaturesCol("features")
    .setLabelCol("label")

  val splitData = dataModel.randomSplit(Array(0.7, 0.3))
  var (trainingData, testData) = (splitData(0), splitData(1))

  trainingData = trainingData.select("features", "label")
  trainingData.groupBy("label").count.show()
  testData = testData.select("features", "label")
  testData.groupBy("label").count.show()

  // Fit the model
  val lrModel = lr.fit(trainingData)

  println("lrModel done")

  val predictions = lrModel.transform(testData)

  println("predictions done")

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
