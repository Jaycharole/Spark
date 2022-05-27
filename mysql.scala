package RDDbas

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import java.util.Properties
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.SaveMode


object mysql extends App {
  
  val sparkSession = SparkSession.builder()
    .appName("working with mysql")
    .master("local")
    .getOrCreate()
    
    Logger.getRootLogger().setLevel(Level.ERROR)
    val url = "jdbc:mysql://localhost:3306"
    val table = "mydb.sharedata"
    
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "1234")
    
    Class.forName("com.mysql.jdbc.Driver")
    
    val csvRDD1 = sparkSession.read.option("header", "true").option("inferSchema", "true")
        .csv("/home/hadoop/TOPTEN.csv")
        
        val ownSchema = StructType(
            StructField("Code",StringType,true)::
            StructField("Company",StringType,true)::
            StructField("Holding",DoubleType,true)::Nil)
            
        val ShareDF = sparkSession.read.option("header","true").schema(ownSchema)
        .csv("/home/hadoop/TOPTEN.csv")
        
        println("=====> record in DF:" +ShareDF.count())
        
        ShareDF.write.mode(SaveMode.Overwrite).jdbc(url, table,properties)
        
        
        
        
//    val mySqlDF = sparkSession.read.jdbc(url, table,properties)
//    mySqlDF.show()
//    print("**************************************************")
//    val dnoWise = mySqlDF.select("dno").groupBy("dno").count()
//    dnoWise.show()
//    print("With Query and String Mod")
//    val query = "select dno,count(*) as dnoCount from mydb.employee group by dno"
//    val queryR = sparkSession.read.jdbc(url,s"($query) as dnotable",properties)
//    queryR.show()
    
    // Writing Spark to my
    
//    You have emp.log file on hdfs.Using spark find out people who have highest
        
//        
//      
//        
}