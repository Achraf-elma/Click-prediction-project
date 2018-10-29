import org.apache.spark.sql.SparkSession

object Main extends App{

  val context = SparkSession
    .builder()
    .appName("Word count")
    .master("local")
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames


  var data = context.read.json("./data-students.json")
  //data.show()
  //data.printSchema()

  //Clean variable "network"
  //Put values ​​with low occurrence in the other category
  val filterNetworkByOccurrence = data.groupBy("network").count()
    .filter("count >= 500")
    .sort("count")
    .select("network")
    .collect()

  import org.apache.spark.sql.functions._

  var updatedDf = data
  filterNetworkByOccurrence.foreach(row =>{
    if(row.toString()!="[null]"){
      updatedDf = data.withColumn("network", regexp_replace(col("network"), row(0).toString(), "amin"))
    }
  })
  //updatedDf = data.withColumn("network", regexp_replace(col("network"), println(row(0)).toString, "amin"))
  updatedDf.show(200)


  //val newsdf = data.withColumn("network", when(filterNetworkByOccurrence.contains(col("network")), "Other").otherwise(col("network")))

  //data = data.toDF()

  //val filtered = data.groupBy("network").count()
  //val filter = filtered.filter(($"count" < 10320))

    //.sort( $")count".desc).select("network").show(10,false)
  //val newsdf = df.withColumn("network", when(deleteNetwork.contains(col("network")), "Other").otherwise(col("network")))
  //newsdf.show()


}
