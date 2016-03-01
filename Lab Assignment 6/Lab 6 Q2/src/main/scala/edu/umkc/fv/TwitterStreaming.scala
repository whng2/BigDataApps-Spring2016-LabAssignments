package edu.umkc.fv

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by whng2 on 2/28/16.
  */
object TwitterStreaming {

  def main(args: Array[String]) {
    val filters = args

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials

    System.setProperty("twitter4j.oauth.consumerKey", "mnN5ZuPFLK5FE4qWlWRWCx4o4")
    System.setProperty("twitter4j.oauth.consumerSecret", "Dn18BbpK5Mjyy6KzMUqxfJNPyWE1rQ7G97F3vEj9o3jWo2fhJi")
    System.setProperty("twitter4j.oauth.accessToken", "2681346673-6tOHPKSl8YycjtxEm19BYUi32AFzVONFQ2qlArS")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "LYOpQXIuNGFzpa6uOkiFt4ZAqzoMg3q8Tin9EFtDQYj6f")



    //Create a spark configuration with a custom name and master
    // For more master configuration see  https://spark.apache.org/docs/1.2.0/submitting-applications.html#master-urls
    val sparkConf = new SparkConf().setAppName("TwitterStreaming").setMaster("local[*]")
    //Create a Streaming COntext with 10 second window
    val ssc = new StreamingContext(sparkConf, Seconds(30))
    //Using the streaming context, open a twitter stream (By the way you can also use filters)
    //Stream generates a series of random tweets
    val stream = TwitterUtils.createStream(ssc, None, filters)
  //  stream.print()



    val trainingScienceStream = stream.filter(_.getHashtagEntities.mkString.contains("science")).map(_.getText)
    val trainingHealthStream = stream.filter(_.getHashtagEntities.mkString.contains("health")).map(_.getText)
    val trainingOthersStream = stream.filter(!_.getHashtagEntities.mkString.contains("science")).filter(!_.getHashtagEntities.mkString.contains("health")).map(_.getText)

    val trainingScience = trainingScienceStream.foreachRDD(rdd =>
    { val count = rdd.count()
      if (count > 0){
        rdd.repartition(1).saveAsTextFile("data/training/hashtag.science")
      }
    })
    val trainingHealth = trainingHealthStream.foreachRDD(rdd =>
    { val count = rdd.count()
      if (count > 0) {
        rdd.repartition(1).saveAsTextFile("data/training/hashtag.health")
      }
    })
    val trainingOthers = trainingOthersStream.foreachRDD((rdd,time) =>
    { val count = rdd.count()
      if (count > 0) {
        rdd.repartition(1).saveAsTextFile("data/training/hashtag.other")
        val temp = count
      }
    })

    ssc.start()
    ssc.awaitTermination();
  }

}