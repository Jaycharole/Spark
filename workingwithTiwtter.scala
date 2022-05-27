package RDDbas

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.Status
import org.apache.spark.streaming.twitter.TwitterUtils

object workingwithTiwtter{
  
  def main(args: Array[String]): Unit ={ 
     Logger.getRootLogger().setLevel(Level.ERROR)
      if (args.length<4){
       
       System.exit(1)
     }
     val appName = "Twitter Data"
     val conf = new SparkConf()
   
     conf.setAppName(appName).setMaster("local[2]")
     val ssc = new StreamingContext(conf,Seconds(5))
//     val ck = ""
//     val cs = ""
//     val at = ""
//     val ats = ""
//     val Array
     
     val Array(consumerKey,consmerSerect,accessToken,accessTokenSerect) = args.take(4)
     val filters = args.takeRight(args.length - 4)
     val cb = new ConfigurationBuilder
     cb.setDebugEnabled(true)
     .setOAuthConsumerKey(consumerKey)
     .setOAuthConsumerSecret(consmerSerect)
     .setOAuthAccessToken(accessToken)
     .setOAuthAccessTokenSecret(accessTokenSerect)
     
     val auth = new OAuthAuthorization(cb.build())
     val tweets = TwitterUtils.createStream(ssc,Some(auth),filters)
     
     println("====================>Ctreaing tweets stream and Saving")
     
     tweets.saveAsTextFiles("/tweets", "json")
     
     ssc.start()
     ssc.awaitTermination()
     
  }
}