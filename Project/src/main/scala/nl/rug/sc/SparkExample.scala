package nl.rug.sc

import org.bson.Document
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
import java.util
import java.util.{ArrayList, Properties}

import com.mongodb.spark.rdd.MongoRDD
import com.typesafe.config.ConfigFactory
import nl.rug.sc.recommendalgo.{Recommend, predictor}
import nl.rug.sc.misc._
import org.apache.commons.io.Charsets
import org.spark_project.guava.io.BaseEncoding

import scala.io.Source
import scala.util.parsing.json.JSON
import scalaj.http.{Http, HttpOptions}

case class Person(id: Int, name: String, grade: Double) // For the advanced data set example, has to be defined outside the scope

class SparkExample(sparkSession: SparkSession, pathToCsv: String, streamingContext: StreamingContext) {
  private val sparkContext = sparkSession.sparkContext
  private val conf = ConfigFactory.load()
  private val kafka_server = conf.getString("spark-project.kafka.server")
  private val streaming_source = conf.getString("spark-project.kafka.source")
  private val DB = conf.getString("spark-project.mongo.DB")
  private val historyColl = conf.getString("spark-project.mongo.historyCollection")
  private val resultsColl = conf.getString("spark-project.mongo.modelCollection")
  private val tripletsColl = conf.getString("spark-project.mongo.dataCollection")

  private val TOPIC = "test"
  private val  props = new Properties()
  private val recommender = new Recommend()
  props.put("bootstrap.servers", kafka_server)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "group1")


  /**
    * An example using RDD's, try to avoid RDD's
    */
  def rddExample(): Unit = {
    val data = List(1, 2, 3, 4, 5)
    val rdd = sparkContext.parallelize(data)

    rdd
      .map(x => x + 1) // Increase all numbers by one (apply the transformation x => x + 1 to every item in the set)
      .collect() // Collect the data (send all data to the driver)
      .foreach(println) // Print each item in the list

    printContinueMessage()
  }

  /**
    * An example using Data Frames, improvement over RDD but Data Sets are preferred
    */
  def dataFrameExample(): Unit = {
    import sparkSession.implicits._ // Data in dataframes must be encoded (serialized), these implicits give support for primitive types and Case Classes
    import scala.collection.JavaConverters._

    val schema = StructType(List(
      StructField("number", IntegerType, true)
    ))

    val dataRow = List(1, 2, 3, 4, 5)
      .map(Row(_))
      .asJava

    val dataFrame = sparkSession.createDataFrame(dataRow, schema)

    dataFrame
      .select("number")
      .map(row => row.getAs[Int]("number")) // Dataframe only has the concept of Row, we need to extract the column "number" and convert it to an Int
      .map(_ + 1) // Different way of writing x => x + 1
      .collect()
      .foreach(println)

    dataFrame.printSchema() // Data frames and data sets have schemas

    printContinueMessage()
  }

  /**
    * An example using Data Sets, improvement over both RDD and Data Frames
    */
  def dataSetExample(): Unit = {
    import sparkSession.implicits._// Data in datasets must be encoded (serialized), these implicits give support for primitive types and Case Classes

    val dataSet = sparkSession.createDataset(List(1, 2, 3, 4, 5))

    dataSet
      .map(_ + 1) // Different way of writing x => x + 1
      .collect()
      .foreach(println)

    dataSet.printSchema()

    printContinueMessage()
  }

  /**
    * Advanced data set example using Scala's Case Classes for more complex data, note that the Case Class is defined at the top of this file.
    */
  def dataSetAdvancedExample(): Unit = {
    import sparkSession.implicits._

    val dataSet = sparkSession.createDataset(List(
      Person(1, "Alice", 5.5),
      Person(2, "Bob", 8.6),
      Person(3, "Eve", 10.0)
    ))

    dataSet
      .show() // Shows the table

    printContinueMessage()

    dataSet
      .map(person => person.grade) // We transform the Person(int, string, double) to a double (Person => Double), extracting the person's grade
      .collect() // Collect in case you want to do something with the data
      .foreach(println)

    printContinueMessage()

    // Even cleaner is
    dataSet
      .select("grade")
      .show()

    dataSet.printSchema()

    printContinueMessage()
  }

  /**
    * In your case, you will be reading your data from a database, or (csv, json) file, instead of creating the data in code as we have previously done.
    * We use a CSV containing 3200 US cities as an example data set.
    */
  def dataSetRealisticExample(): Unit = {
    val dataSet = sparkSession.read
      .option("header", "true") // First line in the csv is the header, will be used to name columns
      .option("inferSchema", "true") // Infers the data types (primitives), otherwise the schema will consist of Strings only
      .csv(pathToCsv) // Loads the data from the resources folder in src/main/resources, can also be a path on your storage device

    dataSet.show(50) // Show first 20 results

    printContinueMessage()

    dataSet
      .sort(desc("count")) // Sort by play count
      .show(50)

    printContinueMessage()

    import sparkSession.implicits._ // For the $-sign notation

    //    dataSet
    //      .filter($"pop" < 10000) // Filter on column 'pop' such that only cities with less than 10k population remain
    //      .sort($"lat") // Sort remaining cities by latitude, can also use $-sign notation here
    //      .show()

    //    printContinueMessage()

    //    dataSet
    //      .withColumn("name_first_letter", $"name".substr(0,1)) // Create a new column which contains the first letter of the name of the city
    //      .groupBy($"name_first_letter") // Group all items based on the first letter
    //      .count() // Count the occurrences per group
    //      .sort($"name_first_letter") // Sort alphabetically
    //      .show(26) // Returns the number of cities in the US for each letter of the alphabet, shows the first 26 results

    //    printContinueMessage()

    import org.apache.spark.sql.functions._ // For the round  (...) functionality

    //    dataSet
    //      .withColumn("pop_10k", (round($"pop" / 10000) * 10000).cast(IntegerType)) // Create a column which rounds the population to the nearest 10k
    //      .groupBy("pop_10k") // Group by the rounded population
    //      .count() // Count the occurences
    //      .sort("pop_10k") // Sort from low to high
    //      .show(100) // Returns the number of cities in 10k intervals

    dataSet.printSchema()

    printContinueMessage()
  }

  def mongoData(): Unit = {
    //val rdd = MongoSpark.load(sparkSession.sparkContext)
    val dataSet = MongoSpark.load(sparkSession)
    dataSet.printSchema()
    println(dataSet.count())

    println(dataSet.first().toString())

    printContinueMessage()

    val filteredDataSet = dataSet.filter(dataSet("count")>5)
    filteredDataSet.show(numRows = 100, truncate = false)
    printContinueMessage()

    MongoSpark.save(filteredDataSet)

    printContinueMessage()
  }

  def streamMQSpark(): Unit = {
    println("Here1")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafka_server,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    println("Here2")
    stream.map(record => (record.key, record.value)).print()
    streamingContext.start()
    streamingContext.awaitTermination()

    println("Here3")
  }

  def kafkaProducer(): Unit = {
    import org.apache.kafka.clients.producer._
    val producer = new KafkaProducer[String, String](props)
    for(i<- 1 to 20){
      val record = new ProducerRecord(TOPIC, "key", s"hello $i")
      val result = producer.send(record)
      println(record)
    }

    val record = new ProducerRecord(TOPIC, "key", "the end "+new java.util.Date)
    producer.send(record)

    producer.close()
  }

  def kafkaStreamProducer(): Unit = {
    import org.apache.kafka.clients.producer._
    val producer = new KafkaProducer[String, String](props)
    val path = getClass.getResource(streaming_source).getPath
    val bufferedSource = Source.fromFile(path)
    for (line <- bufferedSource.getLines){
      val cols = line.split(",").map(_.trim)
      println(cols(1))
      val record = new ProducerRecord(TOPIC, "key", cols(1))
      val result = producer.send(record)
      Thread.sleep(2500)
    }
    bufferedSource.close()
    producer.close()
  }

  def kafkaConsumer(): Unit = {
    val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(util.Collections.singletonList(TOPIC))

    while(true){
      val records=consumer.poll(2000)
      println(records.count())
      for (record<-records.asScala){
        println(record)
      }
    }
  }

  def spotifyToken(): Unit = {
    val client_id = conf.getString("spark-project.spotify.client_id")
    val client_secret = conf.getString("spark-project.spotify.client_secret")
    val unencodedAuth = client_id + ':' + client_secret

    val encoded_auth = BaseEncoding.base64().encode(unencodedAuth.getBytes(Charsets.UTF_8))
    val result = Http("https://accounts.spotify.com/api/token\n")
      .postData("grant_type=client_credentials")
      .header("Content-Type", "application/x-www-form-urlencoded")
      .header("Charset", "UTF-8")
      .header("Authorization", "Basic " + encoded_auth)
      .option(HttpOptions.readTimeout(10000)).asString
    println("-------------")

    if (result.code != 200){
      println("Error getting the access token from Spotify")
      return
    }

    println("ok")
    val binResponse = JSON.parseFull(result.body)
    val map = binResponse.get.asInstanceOf[Map[String, Any]]
    val accessToken = map("access_token")
    println(accessToken)
  }

  def randomSample(percent:Double): Unit = {
    val readConfig = ReadConfig(Map("database" -> DB, "collection" -> tripletsColl, "readPreference.name" -> "Primary"), Some(ReadConfig(sparkContext)))
    val dataSet = MongoSpark.load(sparkContext, readConfig).toDF()
    val sampledDS = dataSet.sample(withReplacement = false, percent)

//    val writeConfig = WriteConfig(Map("database" -> "music_data2", "collection" -> "triplets", "writeConcern.w" -> "majority"), Some(WriteConfig(sparkContext)))
//    MongoSpark.save(sampledDS, writeConfig)
    MongoSpark.write(sampledDS).option("database", DB).option("collection", tripletsColl)
      .option("writeConcern.w", "majority").mode("overwrite").save()
    printContinueMessage()
  }

  def fmTrainingExample(k: Int):Unit = {
    val readConfig = ReadConfig(Map("database" -> DB, "collection" -> tripletsColl, "readPreference.name" -> "Primary"), Some(ReadConfig(sparkContext)))
    val dataSet = MongoSpark.load(sparkContext, readConfig)
    val myRdd: RDD[Document] = dataSet.rdd
    //    val myRdd = MongoSpark.load(sparkSession.sparkContext)
    println()
    println("=================================")
    println(dataSet.getClass)
    println("=================================")
    println()

    val model = recommender.train(sparkContext, myRdd, k)
    val toSave = model.map{ strDenVec =>
      //      new Document("_id", strDenVec._1).append("vectors", strDenVec._2.toArray.toList.asJava)
      Row(strDenVec._1, strDenVec._2.toArray)
    }

    val df = sparkSession.sqlContext.createDataFrame(toSave, StructType(
      StructField("_id", StringType, nullable = false)::
        StructField("vectors", ArrayType(DoubleType, containsNull = false), nullable = false)::Nil)
    )
    MongoSpark.write(df).option("database", DB).option("collection", resultsColl).mode("overwrite").save()
    //    MongoSpark.save(toSave)
    printContinueMessage()
  }

  def predictExample(songId: String, k: Int): Unit = {
    val readConfigArchived = ReadConfig(Map("database" -> DB, "collection" -> historyColl, "readPreference.name" -> "Primary"), Some(ReadConfig(sparkContext)))
    val historyRequests = MongoSpark.load(sparkContext, readConfigArchived).toDF()

    val requestedCandidates = if (historyRequests.head(1).isEmpty) null else historyRequests.filter(historyRequests("_id").equalTo(songId))
    val count = if (requestedCandidates == null) 0 else requestedCandidates.count()

    println("========"+count)
    if(count > 0){
      val histObj = requestedCandidates.first()
      println("Request song ID: " + histObj.getString(0))
      for (i <- 0 until histObj.getList(1).size()-1){
        println(histObj.getList(1).get(i))
      }

    }else {
      val readConfig = ReadConfig(Map("collection" -> resultsColl, "readPreference.name" -> "Primary"), Some(ReadConfig(sparkContext)))
      val customRdd = MongoSpark.load(sparkContext, readConfig)

      val results = recommender.predictBySongId(songId, customRdd, k).map { obj =>
        new Document("songId", obj._1).append("distance", obj._2).append("vectors", obj._3.toList.asJava)
      }.toList.asJava

      val arr = Array(songId)
      val toSave = sparkContext.parallelize(arr).map { obj =>
        new Document("_id", obj).append("candidateSongs", results)
      }
      val writeConfig = WriteConfig(Map("database" -> DB, "collection" -> historyColl, "writeConcern.w" -> "majority"), Some(WriteConfig(sparkContext)))
      MongoSpark.save(toSave, writeConfig)
    }

  }


  private def printContinueMessage(): Unit = {
    println("Check your Spark web UI at http://localhost:4040 and press Enter to continue. [Press Backspace and Enter again if pressing enter once does not work]")
    scala.io.StdIn.readLine()
    println("Continuing, please wait....")
  }


}


