package edu.umkc.fv

import edu.umkc.fv.NLPUtils._
import edu.umkc.fv.Utils._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Mayanka on 14-Jul-15.
  */
object FeatureVector1 {

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
     val sparkConf = new SparkConf().setAppName("TwitterStreaming").setMaster("local[*]").setAppName("FeatureVector1").set("spark.driver.memory", "3g").set("spark.executor.memory", "3g")
     //Create a Streaming COntext with 10 second window
     val ssc = new StreamingContext(sparkConf, Seconds(30))
     //Using the streaming context, open a twitter stream (By the way you can also use filters)
     //Stream generates a series of random tweets
     val stream = TwitterUtils.createStream(ssc, None, filters)
     stream.print()



     //Testing data
     val testing = stream.filter(!_.getHashtagEntities.isEmpty).map(_.getText)

     val testingData = testing.foreachRDD(rdd =>
     { val count = rdd.count()
       if (count > 0){
         rdd.saveAsTextFile("data/test/")
       }
     })

     ssc.start()
     ssc.awaitTerminationOrTimeout(10)


     val sc = ssc.sparkContext
     val stopWords = sc.broadcast(loadStopWords("/stopwords.txt")).value
     val labelToNumeric = createLabelMap("data/training/")
     println(labelToNumeric)

     var model: NaiveBayesModel = null


     // Training the data
     val training = sc.wholeTextFiles("data/training/*")
       .map(rawText => createLabeledDocument(rawText, labelToNumeric, stopWords))
     val X_train = tfidfTransformer(training)
     X_train.foreach(vv => println(vv))

     model = NaiveBayes.train(X_train, lambda = 1.0)

     val lines=sc.wholeTextFiles("data/test/*")
     val data = lines.map(line => {

         val test = createLabeledDocumentTest(line._2, labelToNumeric, stopWords)
         println(test.body)
         test


     })
          val X_test = tfidfTransformerTest(sc, data)

            val predictionAndLabel = model.predict(X_test)
            println("PREDICTION")
            predictionAndLabel.foreach(x => {
              labelToNumeric.foreach { y => if (y._2 == x) {
                println(y._1)
              }
              }
            })
   }
 }
