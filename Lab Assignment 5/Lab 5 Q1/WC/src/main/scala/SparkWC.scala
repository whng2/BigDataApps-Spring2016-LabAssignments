

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.{SparkContext, SparkConf}

object SparkWC {

  def main(args: Array[String]) {


    val sparkConf = new SparkConf().setAppName("SparkWordCount222").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val input=sc.textFile("input.txt")

    val wc=input.flatMap(line=>{line.split(" ")}).map(word=>(word,1)).cache()

    val output=wc.reduceByKey(_+_)

    output.saveAsTextFile("output")

    val o=output.collect()

    var s:String="Words:Count \n"
    o.foreach{case(word,count)=>{

      s+=word+" : "+count+"\n"

    }}

    //SocketClient.sendCommandToRobot(s)
  }

}
