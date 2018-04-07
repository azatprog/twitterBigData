import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.{Row, SparkSession, functions}


object Main extends App {
  val conf = new SparkConf().setMaster("local[2]").setAppName("Spark CSV Reader")
  val ssc = new StreamingContext(conf, Seconds(15))

  //  val sc = org.apache.spark.sql.SparkSession.builder
  //    .master("local")
  //    .appName("Spark CSV Reader")
  //    .getOrCreate

  val w2vModel = Word2VecModel.load("Word2Vec")
  val lrModel = LogisticRegressionModel.load("LogisticRegression")

  //  val ssc = new StreamingContext(sc, Seconds(15))
  case class Tweet(createdAt: Long, text: String)

  val stream = TwitterUtils.createStream(ssc, None)
  val twits = stream.window(Seconds(60)).map(m =>
    Tweet(m.getCreatedAt().getTime() / 1000, m.getText)
    //    Tweet(1, "love good happy")
  )


  twits.foreachRDD { rdd =>
    // Get the singleton instance of SparkSession
    val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
    import spark.implicits._

    val toArray = functions.udf[Array[String], String](_.split(" ")
      .filter(_.length > 2)
      .filter(!_.startsWith("@"))
      .map(str => str
        .replaceAll("[^A-Za-z0-9]", "")
        .toLowerCase()
      )
    )

    // Convert RDD[String] to DataFrame
    val wordsDataFrame = rdd.toDF("createdAt", "text")
    val trans = w2vModel.transform(wordsDataFrame
      .withColumn("array", toArray(wordsDataFrame("text"))))

    lrModel.transform(trans)
      .select("text", "prediction")
      .collect()
      .foreach { case Row(text: String, prediction: Double) =>
        println("blablablah", text, prediction)
      }

  }
    ssc.start()
    ssc.awaitTermination()
}
