import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator,CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoderEstimator, VectorAssembler}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{explode, split, udf, when}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.util
import org.apache.spark.ml._
import org.apache.spark.ml.classification.RandomForestClassificationModel

/**
  * The programm that predict if a user clicks on an or not
  */

object Main extends App{

  val context = SparkSession
    .builder()
    .appName("Word count")
    .master("local")
    .getOrCreate()

    def createModel(): Unit ={



      context.sparkContext.setLogLevel("WARN")

      // this is used to implicitly convert an RDD to a DataFrame.


      //select your variable to add and change inside the variable columnVectorialized and dataModel at the end of the code
      val untreatedData = context.read.json("./src/main/scala/data-students.json").select("appOrSite", "network", "type", "publisher","size", "label", "interests", "user")

      val df = untreatedData.withColumn("label", when(col("label") === true, 1).otherwise(0))
        .withColumn("network", Cleaner.udf_clean_network(untreatedData("network")))
        .withColumn("newSize", when(untreatedData("size").isNotNull,concat_ws(" ", untreatedData("size"))).otherwise("Unknown")).drop("size")
        .withColumn("interests",  when(untreatedData("interests").isNotNull, Cleaner.udf_renameInterestByRow(untreatedData("interests"))).otherwise("null"));


      val cleanData = df.drop("user")


      // Fetching column labels
      val colnames = cleanData.drop("label").schema.fields.map(col => col.name)


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



      // Cross Validation
      println("Cross Validation :")

      // We use a ParamGridBuilder to construct a grid of parameters to search over.
      val paramGrid = new ParamGridBuilder()
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

    }





  def applyModel(path : String): Unit ={



    context.sparkContext.setLogLevel("WARN")

    // this is used to implicitly convert an RDD to a DataFrame.


    //select your variable to add and change inside the variable columnVectorialized and dataModel at the end of the code
    val untreatedData = context.read.json(path).select("appOrSite", "network", "type", "publisher","size", "interests", "user")

    val df = untreatedData.withColumn("network", Cleaner.udf_clean_network(untreatedData("network")))
      .withColumn("newSize", when(untreatedData("size").isNotNull,concat_ws(" ", untreatedData("size"))).otherwise("Unknown")).drop("size")
      .withColumn("interests",  when(untreatedData("interests").isNotNull, Cleaner.udf_renameInterestByRow(untreatedData("interests"))).otherwise("null"));


    val cleanData = df.drop("user")


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
    println("pipeline done")
    val dfEncoded = pipeline.fit(cleanData).transform(cleanData)
    println("encoded data done")

    val renamedEncoded = colnames.map(col => col + "Encode")

    //Add your variable inside the setInputCols by adding Encode after
    val columnVectorialized = new VectorAssembler()
      .setInputCols(renamedEncoded)
      .setOutputCol("features")



    val dataToPredict = columnVectorialized.transform(dfEncoded).select( "features")

    val LoadModel =  CrossValidatorModel.load("model")


    // Make predictions on test documents. cvModel uses the best model found (lrModel).

    val predictions = LoadModel.transform(dataToPredict)

    println("prediction done")

    predictions.show()



    println("evaluation")




  }

  try{
    if(args.isEmpty){
      println("You must enter the path of the file to predict in parameter")
    }else{

      applyModel(args(0))
    }
  }catch{
    //If there is no model to load
    case e: org.apache.hadoop.mapred.InvalidInputException => {
      println("No model found, creating one")
      createModel()

      applyModel(args(0))
    }
  }
  context.stop()
}