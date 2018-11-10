import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer,OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.classification.LogisticRegression



//Don't pay attention of red messages INFO (it's not an error)
/**
  * The programm that predict if a user clicks on an or not
  */
object Main extends App{

  val context = SparkSession
    .builder()
    .appName("Word count")
    .master("local")
    .getOrCreate()

  
  //Put your own path to the json file
  val untreatedData = context.read.json("./src/resources/data-students.json").select("network","city","impid","exchange","media","os","type", "label")


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
  //untreatedData.show()
  //data.show()

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

  // Fit the model
  val lrModel = lr.fit(dataModel)
  // Print the coefficients and intercept for logistic regression
  println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")


  context.stop()
}
