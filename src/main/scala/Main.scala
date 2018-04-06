import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}

object Main extends App {
  val sc = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate

  val w2vModel = Word2VecModel.load("Word2Vec")
  val lrModel = LogisticRegressionModel.load("LogisticRegression")

//  val ssc = new StreamingContext(sc, Seconds(15))
//  val stream = TwitterUtils.createStream(ssc, None)
//  case class Tweet(createdAt:Long, text:String)
//  val twits = stream.window(Seconds(60)).map(m=>
//    Tweet(m.getCreatedAt().getTime()/1000, m.toString)
//  )
//
//  twits.foreachRDD(rdd => rdd.collect().foreach(println))
//  ssc.start()
//  ssc.awaitTermination()
}
