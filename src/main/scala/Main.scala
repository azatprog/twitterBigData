import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming._
import org.apache.spark.SparkConf

object Main extends App {
  val sc = new SparkConf().setAppName("demo").setMaster("local[2]") // local
  val ssc = new StreamingContext(sc, Seconds(15))
  val stream = TwitterUtils.createStream(ssc, None)
  case class Tweet(createdAt:Long, text:String)
  val twits = stream.window(Seconds(60)).map(m=>
    Tweet(m.getCreatedAt().getTime()/1000, m.toString)
  )
  twits.foreachRDD(rdd => rdd.collect().foreach(println))
  ssc.start()
  ssc.awaitTermination()

}
