import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{explode, split, udf, when}

/**
  * The programm that predict if a user clicks on an or not
  */


object Test extends App  {

  def udf_clean_size = {
    udf {(s: Any) =>
      s match {
        case Array(x,y) => "[" + x.toString + "," + y.toString + "]";
        case _ => "Other";
      }
    }
  }

  val context = SparkSession
    .builder()
    .appName("Word count")
    .master("local")
    .getOrCreate()

  context.sparkContext.setLogLevel("WARN")

  // this is used to implicitly convert an RDD to a DataFrame.
  import org.apache.spark.sql.functions._

  //Put your own path to the json file
  //select your variable to add and change inside the variable columnVectorialized and dataModel at the end of the code
  val untreatedData = context.read.json("./src/main/scala/data-students.json").select("size")
  val sizeTreated = untreatedData.withColumn("size", udf_clean_size(untreatedData("size")))
  sizeTreated.show();
}
object Main {

  val context = SparkSession
    .builder()
    .appName("Word count")
    .master("local")
    .getOrCreate()

  context.sparkContext.setLogLevel("WARN")

  // this is used to implicitly convert an RDD to a DataFrame.
  import org.apache.spark.sql.functions._

  //select your variable to add and change inside the variable columnVectorialized and dataModel at the end of the code
  val untreatedData = context.read.json("./src/resources/data-students.json").select("appOrSite", "network", "type", "publisher", "label", "interests", "user")

  val df = untreatedData.withColumn("label", when(col("label") === true, 1).otherwise(0))
    .withColumn("network", Cleaner.udf_clean_network(untreatedData("network")))

  df.printSchema()

  val cleanedInterests = df
  val cleanData = cleanedInterests.drop("user")
  cleanData.show()

  // Fetching column labels
  val colnames = cleanData.drop("label").schema.fields.map(col => col.name)
  colnames.map(col => println(col))

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
  println("pipeline done")
  val dfEncoded = pipeline.fit(cleanData).transform(cleanData)
  println("encoded data done")

  val renamedEncoded = colnames.map(col => col + "Encode")

  //Add your variable inside the setInputCols by adding Encode after
  val columnVectorialized = new VectorAssembler()
    .setInputCols(renamedEncoded)
    .setOutputCol("features")

  val dataModel = columnVectorialized.transform(dfEncoded).select("label", "features")

  println("Vector assembler done")

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setFeaturesCol("features")
    .setLabelCol("label")

  //TODO Find a better way to split
  val splitData = dataModel.randomSplit(Array(0.7, 0.3))
  var (trainingData, testData) = (splitData(0), splitData(1))
  trainingData = trainingData.select("features", "label")
  testData = testData.select("features", "label")

  // Fit the model
  val lrModel = lr.fit(trainingData)

  println("lrModel done")

  val predictions = lrModel.transform(testData)

  println("prediction done")

  val evaluator = new BinaryClassificationEvaluator()
    .setMetricName("areaUnderROC")
    .setRawPredictionCol("rawPrediction")
    .setLabelCol("label")

  println(evaluator.evaluate(predictions) + " ************")

  context.stop()
}
