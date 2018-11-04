import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

case class Cleaner() {

  def selectValueFromGivenCount(data: DataFrame, limit: String, column: String) = {
    data.groupBy(column).count()
      .filter("count "+limit)
      .sort("count")
      .select(column)
      //.show(200)
      .collect()
  }


}

object Cleaner{

  /**
    * Function that renders defaultValue if this defaultValue is contained in a given list of String, otherwise it returns the value of the column that is being processed.
    * @param list
    * @param defaultValue
    * @return
    */
  def udf_check(list: Set[String], defaultValue: String) = {
    udf {(s: String) => if(list.contains("["+s+"]")){
      defaultValue
    }
    else{
      s
    }}
  }


}
