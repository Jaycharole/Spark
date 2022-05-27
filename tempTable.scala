package RDDbas

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType




object tempTable {
  
  def main(args: Array[String]): Unit = {
    
    val sparkSession = SparkSession.builder()
    .appName("Veiw Creation")
    .master("local")
    .getOrCreate()
    
    Logger.getRootLogger().setLevel(Level.ERROR)
     
    
        val csvRDD1 = sparkSession.read.option("header", "true").option("inferSchema", "true")
        .csv("/home/hadoop/TOPTEN.csv")
        csvRDD1.printSchema()
         println("....................................................")
   
        val ownSchema = StructType(
            StructField("Code",StringType,true)::
            StructField("Company",StringType,true)::
            StructField("Holding",DoubleType,true)::Nil)
            
        val ShareDF = sparkSession.read.option("header","false").schema(ownSchema)
        .csv("/home/hadoop/TOPTEN.csv")
        
        ShareDF.createTempView("Share_Data")
        val rows = sparkSession.sql("select * from Share_Data where Holding >7.0")
        rows.show()
       
  }
  
}
  
  
