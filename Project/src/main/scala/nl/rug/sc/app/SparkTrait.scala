package nl.rug.sc.app


import nl.rug.sc.SparkExample
import nl.rug.sc.app.SparkLocalMain.sparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

trait SparkTrait { // A trait can be compared to a Java Interface
  def sparkSession: SparkSession // this def has to be implemented when extending this trait
  def pathToCsv: String // path to csv file, also has to be implemented
  def streamingContext: StreamingContext

  def run(): Unit = {
    val example = new SparkExample(sparkSession, pathToCsv, streamingContext)

//    example.mongoData()
//    This is to get the stream directly from Kafka into Spark
//    example.streamMQSpark()

    /* run this to get sampled dataset */
//    example.randomSample(0.01)

    /* run this to get training results */
//    example.fmTrainingExample()

//    example.predictExample("SOSIWOT12AB018E68B")


    example.spotifyToken()

//    val futureCons = Future{
//      example.kafkaConsumer()
//    }
//
//    val futureProd = Future{
//      example.kafkaStreamProducer()
//    }
//
//
//    Await.result(futureCons, Duration.Inf)
//    sparkSession.stop()


    println("Done")
  }
}
