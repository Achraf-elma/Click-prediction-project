import org.apache.spark.sql.{DataFrame, SparkSession}


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
  val data = context.read.json("./src/resources/data-students.json")
  val cleaner = new Cleaner()
  val newData = cleanNetwork(data)
  newData.show()

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

}
