import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, functions}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object MLExample extends App {
  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate
  val df = spark.read
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
    .load("testdata.manual.2009.06.14.csv")
//    .load("training.1600000.processed.noemoticon.csv")

  val toArray = functions.udf[Array[String], String](_.split(" "))

  val needed_df = df
    .drop("id", "date", "query", "user")
    .filter(df("label") =!= 2)
    .withColumn("array", toArray(df("text")))

  val word2Vec = new Word2Vec()
    .setInputCol("array")
    .setOutputCol("features")
    .setVectorSize(7)
    .setMinCount(1)
    .setMaxIter(10)

  val w2vModel = word2Vec.fit(needed_df)

  val result = w2vModel.transform(needed_df)

  w2vModel.save("Word2Vec")

  val Array(train, test) = result.randomSplit(Array(0.8, 0.2), 42)

  val lr = new LogisticRegression()
  lr.setMaxIter(20)
    .setRegParam(0.01)
  val lrModel = lr.fit(train)
  var counter = lrModel.transform(test)
    .select("features", "label", "probability", "prediction")
    .collect
    .map { case Row(_: Vector, label: Integer, _: Vector, prediction: Double) =>
      if (prediction == label) 1 else 0
    }.sum

  lrModel.save("LogisticRegression")

  println(1.0 * counter / test.count())
}