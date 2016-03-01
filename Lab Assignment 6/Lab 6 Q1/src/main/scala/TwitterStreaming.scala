import org.apache.commons.configuration.ConfigurationBuilder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.commons.cli.{Options, ParseException, PosixParser}

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
    stream.print()
    //Map : Retrieving Hash Tags
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    //Finding the top hash Tags on 10 second window
    val topCounts30 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(30))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    // Print popular hashtags
    topCounts30.foreachRDD(rdd => {
      val topList = rdd.take(30)
      println("\nPopular topics in last 30 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}

      var s:String="Popular topics used in last 30 seconds: \nWords:Count \n"
      topList.foreach{case(count,word)=>{

        s+=word+" : "+count+"\n"

      }}
      SocketClient.sendCommandToRobot(s)

    })

    ssc.start()
    ssc.awaitTermination()
  }

}