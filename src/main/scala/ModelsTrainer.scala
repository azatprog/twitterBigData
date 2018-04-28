import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, functions}


object ModelsTrainer extends App {

  def toArray = functions.udf[Array[String], String](_.split(" ")
    .filter(_.length > 2)
    .filter(!_.startsWith("@"))
    .map(str => str
      .replaceAll("[^A-Za-z0-9]", "")
      .toLowerCase()
    )
  )

  def readCSV(spark: SparkSession, file: String) = {
    spark.read
      .format("csv")
      .option("header", "false") //reading the headers
      .option("mode", "DROPMALFORMED")
      .schema(StructType(List(
        StructField("label", IntegerType, nullable = false),
        StructField("id", IntegerType, nullable = false),
        StructField("date", StringType, nullable = false),
        StructField("query", StringType, nullable = false),
        StructField("user", StringType, nullable = false),
        StructField("text", StringType, nullable = false)
      )))
      .load(file)
      .drop("id", "date", "query", "user")
      .filter("label <> 2")
  }

  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate

  /*

  testdata.manual.2009.06.14.csv
   */

  //  var trainTweets = readCSV(spark, "training.1600000.processed.noemoticon.csv")
  //  var testTweets = readCSV(spark, "testdata.manual.2009.06.14.csv")

  var Array(trainTweets, testTweets) = readCSV(spark, "testdata.manual.2009.06.14.csv").randomSplit(Array(0.8, 0.2), 123)

  trainTweets = trainTweets.withColumn("array", toArray(trainTweets("text")))
  testTweets = testTweets.withColumn("array", toArray(testTweets("text")))

  val w2vModel = new Word2Vec()
    .setInputCol("array")
    .setOutputCol("features")
    .setVectorSize(7)
    .setMinCount(1)
    .setMaxIter(10)
    .fit(trainTweets)
  w2vModel.write.overwrite().save("models/Word2Vec")

  val trainW2V = w2vModel.transform(trainTweets)
  val testW2V = w2vModel.transform(testTweets)

  val lrModel = new LogisticRegression()
    .setMaxIter(20)
    .setRegParam(0.01)
    .fit(trainW2V)
  lrModel.write.overwrite().save("models/LogisticRegression")

  var counter = lrModel.transform(testW2V)
    .select("features", "label", "probability", "prediction")
    .collect
    .map { case Row(_: Vector, label: Integer, _: Vector, prediction: Double) =>
      if (prediction == label) 1 else 0
    }.sum

  println(1.0 * counter / testW2V.count())
}