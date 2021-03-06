import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, _}
import akka.http.scaladsl.server.Directives.{handleWebSocketMessages, path, _}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.{Row, SparkSession, functions}
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils

import scala.io.StdIn


object Main extends App {
  implicit val system = ActorSystem("example")
  implicit val materializer = ActorMaterializer()

  var clients = List[ActorRef]()

  def flow: Flow[Message, Message, Any] = {
    val client = system.actorOf(Props(classOf[ClientConnectionActor]))
    clients = client :: clients
    val in = Sink.actorRef(client, 'sinkclose)
    val out = Source.actorRef(8, OverflowStrategy.fail).mapMaterializedValue { a =>
      client ! ('income -> a)
      a
    }
    Flow.fromSinkAndSource(in, out)
  }

  val route = path("ws")(handleWebSocketMessages(flow))
  val bindingFuture = Http().bindAndHandle(route, "localhost", 8789)

  println(s"Server is listening to web-socket at ws://localhost:8789/ws\n Open frontent/index.html and press RETURN to continue...")
  StdIn.readLine()

  import system.dispatcher

  val conf = new SparkConf().setMaster("local[2]").setAppName("Spark CSV Reader")
  val ssc = new StreamingContext(conf, Seconds(15))

  val w2vModel = Word2VecModel.load("models/Word2Vec")
  val lrModel = LogisticRegressionModel.load("models/LogisticRegression")

  //  val ssc = new StreamingContext(sc, Seconds(15))
  case class Tweet(createdAt: Long, text: String)

  val stream = TwitterUtils.createStream(ssc, None, Array("USA", "Trump"))
  val twits = stream.window(Seconds(60)).map(m =>
    Tweet(m.getCreatedAt().getTime() / 1000, m.getText)
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
        clients.foreach(_ ! ('tweet -> (text -> prediction)))
      }

  }
  ssc.start()
  ssc.awaitTermination()

//  bindingFuture
//    .flatMap(_.unbind())
//    .onComplete(_ => system.terminate())
}


class ClientConnectionActor extends Actor {
  var connection: Option[ActorRef] = None

  val receive: Receive = {
    case ('income, a: ActorRef) => connection = Some(a); context.watch(a); connection.get ! TextMessage.Strict("Hello there!")
    case Terminated(a) if connection.contains(a) => connection = None; context.stop(self)
    case 'sinkclose => context.stop(self)

    case ('tweet, (text, prediction)) => {
      println("from actor...", text)
      connection.foreach(_ ! TextMessage.Strict(
        "{\"prediction\":" + prediction.toString + ",\"text\":\"" + text + "\"}"
      ))
    }

    case TextMessage.Strict(t) => connection.foreach(_ ! TextMessage.Strict(s"echo $t"))
    case _ => // ingone
  }

  override def postStop(): Unit = connection.foreach(context.stop)
}
