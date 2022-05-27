package RDDbas

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object folderStream extends App{
  
  val conf = new SparkConf().setMaster("local[2]")
  .setAppName("FloBasedWordCount")
  
  val sc = new StreamingContext(conf,Seconds(3))
  
  
  val lines = sc.textFileStream("Output/res")
  println("=============================> lines Recieved "+lines.print())
  val words = lines.flatMap(_.split(" ")) 
  val pairs = words.map(word => (word,1))
  val wordsCount = pairs.reduceByKey(_+_)
  
  println("+===============================> Word Count: "+wordsCount.print())
  
  ssc.start()
  ssc.awaitTermination()
      
  
}