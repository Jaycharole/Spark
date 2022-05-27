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

object WorkingWithCSV {
  
  def main(args: Array[String]): Unit = {
    
    val sparkSession = SparkSession.builder()
    .appName("RDD with CSV File")
    .master("local")
    .getOrCreate()
    
    Logger.getRootLogger().setLevel(Level.ERROR)
    
//    val csvRDD = sparkSession.sparkContext.textFile("/home/hadoop/cars.csv");
//    
//    println("No. of rows in file:"+csvRDD.count())
//    println("-------------------------")
//    
//    val header = csvRDD.first()
//    val csvRDDWithoutHeader = csvRDD.filter(line => line!=header)
//    csvRDDWithoutHeader.take(5).foreach(println)
//    println("-----------------------9--")
//    
//    //map takes each line and applies a function on it. 
//    val firstColumn = csvRDDWithoutHeader.map(line => line.split(",")(0))
//    firstColumn.take(5).foreach(println) 
//    println("-------------------------")
//    
//    //limitedColumns.take(10).foreach(println)
//    firstColumn.saveAsTextFile("Output/firstColumn")
    
        val csvRDD1 = sparkSession.read.option("header", "true").option("inferSchema", "true")
        .csv("/home/hadoop/TOPTEN.csv")
        csvRDD1.printSchema()
         println("....................................................")
   
        val ownSchema = StructType(
            StructField("Code",StringType,true)::
            StructField("Company",StringType,true)::
            StructField("Holding",DoubleType,true)::Nil)
            
        val cvsDF = sparkSession.read.option("header","false").schema(ownSchema)
        .csv("/home/hadoop/TOPTEN.csv")
        
        cvsDF.printSchema()
        println("....................................................")

        val CompanyDf=cvsDF.select("Company")
        CompanyDf.show(2)
        
    println("....................................................")
    println(cvsDF.schema)
    println("....................................................")

    val colum=cvsDF.columns
    println("Col name:"+colum.mkString("|"))
    
    val topDesc = cvsDF.describe("Holding")
    topDesc.show()
    
     val topDesc1 = cvsDF.describe("Code")
    topDesc1.show()
    
    val filterDF = cvsDF.select("Holding", "Company").where("Holding>7.0")
    filterDF.show()
    
    val filterDF1 = cvsDF.select("Holding", "Company").where("Holding>4.0")
    .groupBy("Holding").count()
    
    filterDF1.show()
    
  }
  
}