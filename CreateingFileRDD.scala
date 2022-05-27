package RDDbas
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object CreateingFileRDD {
  
  def main(args: Array[String]): Unit ={
    
   // Logger.getLogger("org").setLevel(Level.ERROR)
  //Logger.getLogger("akka").setLevel(Level.ERROR)
   
    Logger.getRootLogger.setLevel(Level.ERROR)
    
    val sparkconf= new SparkConf()
    sparkconf.setAppName("Read File RDD")
    sparkconf.setMaster("local")
    val sc= new SparkContext(sparkconf)
    
    val fileRDD=sc.textFile("/home/hadoop/hbase.txt",3)
    println("no of lines in files "+fileRDD.count())
    
    fileRDD.foreach(println)
    
    
  }
  
}