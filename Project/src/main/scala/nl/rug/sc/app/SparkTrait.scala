package nl.rug.sc.app


import nl.rug.sc.SparkExample
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

trait SparkTrait { // A trait can be compared to a Java Interface
  def sparkSession: SparkSession // this def has to be implemented when extending this trait
  def streamingContext: StreamingContext

  def run(): Unit = {
    val example = new SparkExample(sparkSession, streamingContext)

    //    example.mongoData()
    //    This is to get the stream directly from Kafka into Spark
    //    example.streamMQSpark()

    /* run this to get sampled dataset */
    //    example.randomSample(0.1)

    /* run this to get training results */
    if (sparkSession.conf.get("trainFlag").toLowerCase() == "true") {
      example.fmTrainingExample(5)
    }

//    example.predictExample("Radiohead|||Let Down")

    val futureCons = Future{
      example.predictRequestWaitingConsumer()
    }

    val futureProd = Future{
      example.predictStreaming(5)
    }


    Await.result(futureCons, Duration.Inf)

    sparkSession.stop()

    println("Done")
  }
}
