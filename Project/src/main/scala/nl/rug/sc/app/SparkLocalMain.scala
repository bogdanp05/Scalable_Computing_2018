package nl.rug.sc.app
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkLocalMain extends App with SparkTrait {
  private val master = "local[*]" // local[*] means a local cluster with * being the amount of workers, * = 1 worker per cpu core. Always have at least 2 workers (local[2])

  override def sparkSession = SparkSession // Usually you only create one Spark Session in your application, but for demo purpose we recreate them
    .builder()
    .appName("spark-project")
    .master(master)
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/music_data.triplets")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/music_data.results")
    .config("kafkaIP", "localhost:9092")
    .config("trainFlag", "false")
    .getOrCreate()

//    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/music_data.triplets")
//    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/music_data2.triplets")

  override def streamingContext: StreamingContext= new StreamingContext(sparkSession.sparkContext, Seconds(1))

  run() // Run is defined in the tait SparkBootcamp
}