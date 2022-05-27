package RDDbas
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object WorkingWithXML {
  
   def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
    .appName("RDD with XML File")
    .master("local")
    .getOrCreate()
    
    Logger.getRootLogger().setLevel(Level.ERROR)
    val xmlDf = spark.read.format("com.databricks.spark.xml").option("rowtag","BOOK")
    .load("/home/hadoop/hadoop_books.xml")
    xmlDf.show()
    
    println("....................................")
    
    val selectData = xmlDf.select("AUTHOR", "PRICE")
    selectData.show()
    
    println("..............................")
    xmlDf.printSchema()
    
    
    
  }
  
  
}