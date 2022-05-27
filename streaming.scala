package RDDbas
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


object streaming  extends App {
   Logger.getRootLogger().setLevel(Level.ERROR)

  
// creating a local streaming context wth working threads and batch interval of 3 sec
  
  val conf = new SparkConf().setMaster("local[2]")
  .setAppName("NetworkBasedWordCount")
  
  val ssc = new StreamingContext(conf,Seconds(3))
  
  // Create a Dstream that will connect to hostname:port like localhost:4444
  
  val lines = ssc.socketTextStream("localhost", 44444)
  println("=============================> lines Recieved "+lines.print())
  val words = lines.flatMap { _.split(" ") } 
  val pairs = words.map { word => (word,1) }
  val wordsCount = pairs.reduceByKey(_+_)
  
  println("+===============================> Word Count: "+wordsCount.print())
  
  ssc.start()
  ssc.awaitTermination()
      

}