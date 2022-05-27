package RDDbas
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//Object is main entry point and static class of scala.
object CreatingSparkContext {
  //create main method for ojbect
  
  def main(args: Array[String]):Unit = {
    
    // to create a spark context we need to witre spark config
    
    val sparkCong= new SparkConf();
    
    // setting job or application name
    
    sparkCong.setAppName("My frisr spark application")
    
    // running mode
    sparkCong.setMaster("local")
    
    // setting above spark conf for spark context
    
    val sc=new SparkContext(sparkCong)
    val array = Array(1,2,3,4,5,6,7,8)
    val arrayRDD = sc.parallelize(array, 1)
    
    println("no of elements are: "+arrayRDD.count())
    arrayRDD.foreach {println}
    
    
    
  }
  
  
}