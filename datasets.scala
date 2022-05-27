package RDDbas
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

case class Candidate(FNAME:String,LNAME:String,EMAIL:String,MOBILE:Long,CITY:String,
            STATE:String,ZIP:Long)
case class Contact(EMAIL:String,Mobile:Long)      

object datasets {
  
  def main(args: Array[String]): Unit = {
    
    val sparkSession = SparkSession.builder()
    .appName("Veiw Creation")
    .master("local")
    .getOrCreate()
    
    Logger.getRootLogger().setLevel(Level.ERROR)
     
    import sparkSession.implicits._
    
    val candidateDS = sparkSession.read.option("header", "true")
    .option("inferSchema","true")
    .option("charset","UTF8")
    .option("delimiter",",")
    .csv("/home/hadoop/candidates.csv")
    .as[Candidate]
    candidateDS.show()
    print("..........................")
    
    val fliterData = candidateDS.filter(Candidateobj=>Candidateobj.STATE=="MH")
    fliterData.show()
    
    print("---------------------------------------------")
    
    val fliterData1 = candidateDS.where(candidateDS("CITY")==="pune")
    
     fliterData1.show()
   
     print("******************************************************")
     
     val selectData = candidateDS.select("EMAIL", "MOBILE").as[Contact]
    selectData.show()
    
    
  }
  
}