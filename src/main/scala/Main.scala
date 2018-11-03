import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._


//Don't pay attention of red messages INFO (it's not an error)
object Main extends App{

  val context = SparkSession
    .builder()
    .appName("Word count")
    .master("local")
    .getOrCreate()

  //Put your own path to the json file
  var data = context.read.json("./data-students.json")
  //data.show()
  //data.printSchema()

  //Clean variable "network"
  //Put values ​​with low occurrence (under 5000) in the 'Other' category
  val filterNetworkByOccurrence = data.groupBy("network").count()
    .filter("count < 5000")
    .sort("count")
    //.show(200)
    .select("network")
    .collect()
  var updatedDf = data
  //Todo Problem (foreach take only the last result may be try to replace it by map)
  filterNetworkByOccurrence.foreach(row =>{
    if(row.toString()!="[null]"){
      updatedDf = data.withColumn("network", when(col("network") === row(0).toString,"Other").otherwise(col("network")))
    }
  })
  //Todo problem : Should display only network with occurrences greater than 5000
  updatedDf.groupBy("network").count()
    .sort("count")
    .show(200)



}
