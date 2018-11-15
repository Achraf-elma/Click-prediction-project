import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoderEstimator, VectorAssembler}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

/**
  * The programm that predict if a user clicks on an or not
  */

object Main extends App{

  val context = SparkSession
    .builder()
    .appName("Word count")
    .master("local")
    .getOrCreate()

  context.sparkContext.setLogLevel("WARN")

  // this is used to implicitly convert an RDD to a DataFrame.
  import org.apache.spark.sql.functions._

  //select your variable to add and change inside the variable columnVectorialized and dataModel at the end of the code
  val untreatedData = context.read.json(args(0)).select("appOrSite", "network", "type", "publisher","size", "label", "interests", "user").limit(10)

  val df = untreatedData.withColumn("label", when(col("label") === true, 1).otherwise(0))
    .withColumn("network", Cleaner.udf_clean_network(untreatedData("network")))
    .withColumn("newSize", when(untreatedData("size").isNotNull,concat_ws(" ", untreatedData("size"))).otherwise("Unknown")).drop("size")


  df.printSchema()

  df.groupBy("newSize").count.show()

  val cleanedInterests = df.withColumn("interests",  when(df("interests").isNotNull, Cleaner.udf_renameInterestByRow(df("interests")
                                                          )).otherwise("null"));
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

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setFeaturesCol("features")
    .setLabelCol("label")

  // Cross Validation
  println("Cross Validation :")

  // We use a ParamGridBuilder to construct a grid of parameters to search over.
  val paramGrid = new ParamGridBuilder()
    //.addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
    .addGrid(lr.regParam, Array(0.1, 0.01))
    .build()

  // We now treat the Logistic regression as an Estimator, wrapping it in a CrossValidator instance.
  val cv = new CrossValidator()
    .setEstimator(lr)
    .setEvaluator(new BinaryClassificationEvaluator)
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(3)  // Use 3+ in practice

  // Run cross-validation, and choose the best set of parameters.
  val cvModel = cv.fit(dataModel)

  cvModel.write.overwrite().save("model")
  println("Model created & saved")

  val testData = dataModel.limit(100)

  // Prediction
  println("Predection with first 100 rows :")
  testData.show()

  // Make predictions on test documents. cvModel uses the best model found (lrModel).

  val predictions = cvModel.transform(testData)

  println("prediction done")

  predictions
    .select("features", "probability", "prediction")
    .limit(20)
    .collect()
    .foreach { case Row(features: Vector, prob: Vector, prediction: Double) =>
      println(s"($features) --> prob = $prob, prediction = $prediction")
    }

  val evaluator = cv.getEvaluator

  println(evaluator.asInstanceOf[BinaryClassificationEvaluator].getMetricName + " : " + evaluator.evaluate(predictions) + " ************")


  predictions.show()




  context.stop()
}