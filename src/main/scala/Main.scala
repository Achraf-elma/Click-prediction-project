import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.SparkSession

object Main extends App{

  val sqlContext = SparkSession
    .builder()
    .appName("Word count")
    .master("local")
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames


  val textFile = sqlContext.read.json("./data-students.json")

  // Creates a DataFrame having a single column named "line"
  val df = textFile.toDF()
  textFile.printSchema()
  df.show()

}
