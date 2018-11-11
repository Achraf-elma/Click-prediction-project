import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import scala.util.matching.Regex
import java.util.Date
import java.text.SimpleDateFormat

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

def udf_clean_network = {
    udf {(s: String) =>
      /*if(list.contains("["+s+"]")){
            defaultValue
          }
          else{
            s
          }*/
      val ethernet = new Regex("1(.*)")
      val wifi = new Regex("2(.*)")
      val cell1 = new Regex("3(.*)")
      val cell2 = new Regex("4(.*)")
      val cell3 = new Regex("5(.*)")
      val cell4 = new Regex("6(.*)")

      s match {
        case ethernet(x) => "Ethernet"
        case wifi(x) => "Wifi"
        case cell1(x) => "Cellular Network Unknown"
        case cell2(x) => "2G"
        case cell3(x) => "3G"
        case cell4(x) => "4G"
        case _ => "Unknown"
      }
    }
  }

  def udf_clean_timestamp = {
    udf((col: String) => {
      val ts = col.toInt * 1000L
      val df = new SimpleDateFormat("HH")
      val hour = df.format(ts)
        
      hour match{
        case x if (x.toInt >= 0 && x.toInt <= 6) => "night"
        case x if (x.toInt > 20) => "night"
        case x if (x.toInt > 6 && x.toInt <= 12) => "morning"
        case x if (x.toInt > 12 && x.toInt <= 20) => "afternoon"
        case _ => "Other"
      }
    })
  }
}
