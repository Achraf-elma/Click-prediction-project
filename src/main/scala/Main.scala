import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.classification.{LogisticRegression, BinaryLogisticRegressionSummary}
import org.apache.spark.sql.functions.rand
import org.apache.log4j.{Level, Logger}
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
  val untreatedData = context.read.json("./src/resources/data-students.json").select("appOrSite","network", "timestamp", "label")

  val df = untreatedData.withColumn("label", when(col("label") === true, 1).otherwise(0))
  .withColumn("network", Cleaner.udf_clean_network(untreatedData("network")))
  .withColumn("timestamp", Cleaner.udf_clean_timestamp(untreatedData("timestamp")))


 
 /**
 **udf to replace name of interests
 **/
   val udf_renameI = {
    udf { (s: String) =>
      /*if(list.contains("["+s+"]")){
            defaultValue
          }
          else{
            s
          }*/
      val IAB1 = new Regex("IAB1-(.*)")
      val IAB2 = new Regex("IAB2-(.*)")
      val IAB3 = new Regex("IAB3-(.*)")
      val IAB4 = new Regex("IAB4-(.*)")
      val IAB5 = new Regex("IAB5-(.*)")
      val IAB6 = new Regex("IAB6-(.*)")
      val IAB7 = new Regex("IAB7-(.*)")
      val IAB8 = new Regex("IAB8-(.*)")
      val IAB9 = new Regex("IAB9-(.*)")
      val IAB10 = new Regex("IAB10-(.*)")
      val IAB11 = new Regex("IAB11-(.*)")
      val IAB12 = new Regex("IAB12-(.*)")
      val IAB13 = new Regex("IAB13-(.*)")
      val IAB14 = new Regex("IAB14-(.*)")
      val IAB15 = new Regex("IAB15-(.*)")
      val IAB16 = new Regex("IAB16-(.*)")
      val IAB17 = new Regex("IAB17-(.*)")
      val IAB18 = new Regex("IAB18-(.*)")
      val IAB19 = new Regex("IAB19-(.*)")
      val IAB20 = new Regex("IAB20-(.*)")
      val IAB21 = new Regex("IAB21-(.*)")
      val IAB22 = new Regex("IAB22-(.*)")
      val IAB23 = new Regex("IAB23-(.*)")
      val IAB24 = new Regex("IAB24-(.*)")
      val IAB25 = new Regex("IAB25-(.*)")
      val IAB26 = new Regex("IAB26-(.*)")
      /*val IAB27 = new Regex("IAB27-(.*)")
      val IAB28 = new Regex("IAB28-(.*)")
      val IAB29 = new Regex("IAB29-(.*)")*/

      s match {
        case IAB1(x) => "IAB1-"
        case IAB2(x) => "IAB2-"
        case IAB3(x) => "IAB3-"
        case IAB4(x) => "IAB4-"
        case IAB5(x) => "IAB5-"
        case IAB6(x) => "IAB6-"
        case IAB7(x) => "IAB7-"
        case IAB8(x) => "IAB8-"
        case IAB9(x) => "IAB9-"
        case IAB10(x) => "IAB10-"
        case IAB11(x) => "IAB11-"
        case IAB12(x) => "IAB12-"
        case IAB13(x) => "IAB13-"
        case IAB14(x) => "IAB14-"
        case IAB15(x) => "IAB15-"
        case IAB16(x) => "IAB16-"
        case IAB17(x) => "IAB17-"
        case IAB18(x) => "IAB18-"
        case IAB19(x) => "IAB19-"
        case IAB20(x) => "IAB20-"
        case IAB21(x) => "IAB21-"
        case IAB22(x) => "IAB22-"
        case IAB23(x) => "IAB23-"
        case IAB24(x) => "IAB24-"
        case IAB25(x) => "IAB25-"
        case IAB26(x) => "IAB26-"
        /*case IAB27(x) => "IAB27"
        case IAB28(x) => "IAB28"
        case IAB29(x) => "IAB29"*/
        case _ => "Other"
        //case _ => s
      }
    }

  }
 
 
  //TODO: clean interests
  val renamedInterests= untreatedData.withColumn("listInterests", udf_renameI((untreatedData("interests")))).select("listInterests")
  val updatedInterestsArray = renamedInterests.withColumn("uniqueInterests", explode(split(renamedInterests("listInterests"), ","))).select("uniqueInterests").distinct()


  //identify the ones that are not well written
  //val nonIAB = updateInterestsArray.distinct().where(not(updateInterestsArray("listInterests").contains("IAB"))).show()
 
  val arrayOfInterests =  updatedInterestsArray.withColumn("interestsArray", split(updatedInterestsArray("uniqueInterests"), ",")).select( "interestsArray").show()

  //var target = network.withColumn("interestsArray", split(network("interests"), ",")).select("user", "interestsArray", "interests")

  var target = untreatedData.select("user", "interests")

 //list of string that represents the list of renamed interests
  val listOfUniqueInterest: List[String] = updatedInterestsArray.select("uniqueInterests").collect().map(row => row.toString()).toList

 //trying to add column to a dataframe using foldLefst given a list of columns to add and a dataframe 
 /*def addColumnsViaFold(df: DataFrame, columns: List[String]): DataFrame = {

    /*def udf_check(list: List[String]): UserDefinedFunction = {
      udf { (s: String) => {
        if (list.contains(s+"-")) 1 else 0
      }
    }*/

    columns.foldLeft(df)((acc, col) => {
      acc.withColumn(col, acc("interestsArray")(0).contains(col)) //udf_check(List(acc("interestsArray").toString()))(col))
    })
  }

    addColumnsViaFold(target, listOfUniqueInterest).show()*/

  /*for(i <- 0 until listOfUniqueInterest.length-1) {
    target.withColumn(listOfUniqueInterest(i), target("interests").contains(listOfUniqueInterest(i)))
  }*/
  //target.show()

  //var df = target.select("user")
 //result: a list of column that contains, for each user, either 1 if he/she is interested in the concerned interest(ie corresponds to the name of the column)
  val l1 = listOfUniqueInterest.map(x => target.withColumn(x, when(target("interests").contains(x.substring(1, x.size-2))|| target("interests").contains(x.substring(1, x.size-3)), 1).otherwise(0)).select(x))
  
 //TODO: 3 steps left
 //Step1: create a dataframe that contains each element of the list l1 as columns 
 
 //Step2: create a dataframe that combines the daaframe from step1 and the untreated dataframe and joining it by users
 
 //Step2: use directly a vector assembleer to create a vector that contains the column of interests 
 
 
 

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
    .setInputCols(Array("appOrSiteEncode","networkEncode", "timestampEncode"))
    .setOutputCol("features")

  val dataModel = columnVectorialized.transform(dfEncoded).select("appOrSiteEncode","networkEncode", "timestampEncode", "label", "features")

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0)
    .setElasticNetParam(0)
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

  val predictions = lrModel.transform(testData)

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
