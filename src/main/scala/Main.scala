import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer,OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.classification.{LogisticRegression,BinaryLogisticRegressionSummary}
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


  
  //Put your own path to the json file
  val untreatedData = context.read.json("./data-students.json").select("network","city","impid","exchange","media","os","type", "label")


  val cleaner = new Cleaner()
  val newData = cleanNetwork(untreatedData)
  //newData.show()

  /**
    * Clean the column "network"
    * @param data
    * @return
    */
  def cleanNetwork(data: DataFrame): DataFrame = {
    //Clean variable "network"
    //Put values ​​with low occurrence (under 5000) in the 'Other' category
    val networksWithNotEnoughOccurences = cleaner.selectValueFromGivenCount(data, "< 5000", "network");
    val setOfNetworkToClassify: Set[String] = networksWithNotEnoughOccurences.map(x => x.toString()).toSet
    val updatedDF = data.withColumn("networkClass", Cleaner.udf_check(setOfNetworkToClassify, "<5000")(data("network")))//.drop("network")
    /*updatedDF.show()
    updatedDF.filter("networkClass == '<5000'").show()*/

    //Display only network with occurrences greater than or or equal to 5000
    /*updatedDF.groupBy("network").count()
      .sort("count >= 5000")
      .select("network")
      .show(200)*/
    updatedDF
  }


  //TODO: clean interests
  //val updateInterestsArray= data.withColumn("interests", split(data("interests"), ","))
  //val updateInterestsReplaceNews= updateInterestsArray.withColumn("interests", split(data("interests"), ","))
  //updateInterestsArray.show()
  //updateInterests.select("interests").show()



  // this is used to implicitly convert an RDD to a DataFrame.
  import org.apache.spark.sql.functions._
  val df = untreatedData.withColumn("label", when(col("label") === true, 1).otherwise(0))

    val colnames = df.schema.fields.map(col=> col.name)

    val indexers = colnames.map (
      col => new StringIndexer()
      .setInputCol(col)
      .setOutputCol(col + "Index")
      .setHandleInvalid("keep")
    )

    val encoders = colnames.map (
      col => new OneHotEncoder()
      .setInputCol(col + "Index")
      .setOutputCol(col + "Encode")
    )
    val pipeline = new Pipeline().setStages(indexers ++ encoders)
    val dfEncoded = pipeline.fit(df).transform(df)

    val columnVectorialized = new VectorAssembler()
      .setInputCols(Array("networkEncode","cityEncode","impidEncode","exchangeEncode","mediaEncode","osEncode","typeEncode"))
      .setOutputCol("features")

    val dataModel = columnVectorialized.transform(dfEncoded).select("networkEncode","cityEncode","impidEncode","exchangeEncode","mediaEncode","osEncode","typeEncode","label", "features")
  
  val lr = new LogisticRegression()
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)


  def splitDf(df: DataFrame) = {
    val subsetLabelTrue = df.filter(col("label") === 1)
    val subsetLabelFalse = df.filter(col("label") === 0)
    val nTrain = df.count()*0.7
    val nTest = df.count()*0.3
    val training = subsetLabelTrue.orderBy(rand()).limit((nTrain*0.5).toInt).union(subsetLabelFalse.orderBy(rand()).limit((nTrain*0.5).toInt))
    val test = subsetLabelTrue.orderBy(rand()).limit((nTest*0.5).toInt).union(subsetLabelFalse.orderBy(rand()).limit((nTest*0.5).toInt))
    Array(training,test)
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

println("***********************************************************************")
println("objectiveHistory:")
objectiveHistory.foreach(loss => println(loss))

println("***********************************************************************")

println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

println("***********************************************************************")

roc.show()
println(s"areaUnderROC: ${binarySummary.areaUnderROC}")

println("***********************************************************************")

  context.stop()
}
