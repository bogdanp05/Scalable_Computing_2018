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
import java.time.Instant

import com.mongodb.spark.rdd.MongoRDD
import com.typesafe.config.ConfigFactory
import nl.rug.sc.recommendalgo.{Recommend, predictor}
//import nl.rug.sc.misc._
import org.apache.commons.io.Charsets
import org.apache.kafka.clients.producer._
import org.spark_project.guava.io.BaseEncoding

import scala.collection.mutable._
import scala.io.Source
import scala.util.parsing.json.JSON
import scalaj.http.{Http, HttpOptions}

case class Person(id: Int, name: String, grade: Double) // For the advanced data set example, has to be defined outside the scope

class SparkExample(sparkSession: SparkSession, streamingContext: StreamingContext) {
  private val sparkContext = sparkSession.sparkContext
  private val conf = ConfigFactory.load()
//  private val kafka_server = conf.getString("spark-project.kafka.server")
  private val kafka_server = sparkSession.conf.get("kafkaIP")
  println("----------Kafka IP in SparkExample: " + kafka_server)
  private val streaming_source = conf.getString("spark-project.kafka.source")
  private val DBFULL = conf.getString("spark-project.mongo.DBFULL")
  private val DB = conf.getString("spark-project.mongo.DB")
  private val historyColl = conf.getString("spark-project.mongo.historyCollection")
  private val resultsColl = conf.getString("spark-project.mongo.modelCollection")
  private val tripletsColl = conf.getString("spark-project.mongo.dataCollection")

  private val TOPIC = "test"
  private val TOPIC_REQUEST = "d"
  private val TOPIC_RECOMMENDATION = "predictResults"
  private val props = new Properties()
  private val recommender = new Recommend()
  private val k = 5
  props.put("bootstrap.servers", kafka_server)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "group1")
  private val requestProducer = new KafkaProducer[String, String](props)
  private val requestConsumer = new KafkaConsumer[String, String](props)


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

  def findPopular(): RDD[(String ,Int)] = {
    val readConfig = ReadConfig(Map("database" -> DBFULL, "collection" -> tripletsColl, "readPreference.name" -> "Primary"), Some(ReadConfig(sparkContext)))
    val dataRdd = MongoSpark.load(sparkContext, readConfig)

    val filteredDataSet = dataRdd.map(doc => {
      (doc.getString("song"), doc.getString("count").toInt)
    }).reduceByKey((song1, song2) => song1 + song2).filter(aa => aa._2 > 150).sortBy(_._2, ascending = false)
    val topPopular = filteredDataSet.collect().slice(0, 100).map(dd =>
    {
      println(dd)
    })
    return filteredDataSet
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
    val producer = new KafkaProducer[String, String](props)
    val path = getClass.getResource(streaming_source).getPath
    val bufferedSource = Source.fromFile(path)
    for (line <- bufferedSource.getLines){
      val cols = line.split(",").map(_.trim)
      println("Producer: " + cols(1))
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
    println("subscribed")
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
    val readConfig = ReadConfig(Map("database" -> DBFULL, "collection" -> tripletsColl, "readPreference.name" -> "Primary"), Some(ReadConfig(sparkContext)))
    val dataSet = MongoSpark.load(sparkContext, readConfig).toDF()
    val sampledDS = dataSet.sample(withReplacement = false, percent)

//    val writeConfig = WriteConfig(Map("database" -> "music_data2", "collection" -> "triplets", "writeConcern.w" -> "majority"), Some(WriteConfig(sparkContext)))
//    MongoSpark.save(sampledDS, writeConfig)
    MongoSpark.write(sampledDS).option("database", DB).option("collection", tripletsColl)
      .option("writeConcern.w", "majority").mode("overwrite").save()
    printContinueMessage()
  }

  def fmTrainingExample(k: Int):Unit = {
    val readConfig = ReadConfig(Map("database" -> DBFULL, "collection" -> tripletsColl, "readPreference.name" -> "Primary"), Some(ReadConfig(sparkContext)))
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
    MongoSpark.write(df).option("database", DBFULL).option("collection", resultsColl).mode("overwrite").save()
    //    MongoSpark.save(toSave)
    printContinueMessage()
  }

  def buildString(str: String, strings: Array[String]): String = {
    if(strings == null){
      return ""
    }
    return "Track Name: " + str + "\n" + "Recommendation List: " +strings.mkString(", ") + "\n\n"
  }

  def predictExample(songId: String): Array[String] = {
//    val readConfigArchived = ReadConfig(Map("database" -> DB, "collection" -> historyColl, "readPreference.name" -> "Primary"), Some(ReadConfig(sparkContext)))
//    val historyRequests = MongoSpark.load(sparkContext, readConfigArchived).toDF()
//
//    val requestedCandidates = if (historyRequests.head(1).isEmpty) null else historyRequests.filter(historyRequests("_id").equalTo(songId))
//    val count = if (requestedCandidates == null) 0 else requestedCandidates.count()

    if(false){
//      val histObj = requestedCandidates.first().getList(1).asInstanceOf[util.ArrayList[Object]]
//      resList = histObj.toString
      return Array("")

    }else {
      println("-----here predicts")
      val readConfig = ReadConfig(Map("collection" -> resultsColl, "readPreference.name" -> "Primary"), Some(ReadConfig(sparkContext)))
      val customRdd = MongoSpark.load(sparkContext, readConfig)

      val results = recommender.predictBySongId(songId, customRdd, k)
      if(results == null){
        return null
      }
      val candidates = results.map { obj =>
        new Document("songId", obj._1).append("distance", obj._2).append("vectors", obj._3.toList.asJava)
      }.take(20).toList.asJava

//      val resList = results.map { obj =>
//        obj._1
//      }.collect().slice(0, 10)

//      println(resList.mkString(","))
//      return resList

      /* === save predicted results to database === */

      val arr = Array(songId)
      val toSave = sparkContext.parallelize(arr).map { obj =>
        new Document("_id", obj).append("candidateSongs", candidates)
      }
      val writeConfig = WriteConfig(Map("database" -> DBFULL, "collection" -> historyColl, "writeConcern.w" -> "majority"), Some(WriteConfig(sparkContext)))
      MongoSpark.save(toSave, writeConfig)
      return Array("")
    }
  }

  def songPredictStreamProducer(songId: String, timestamp: Long, hash: Int): Unit = {
    println("Producer: " + songId)
    val record = new ProducerRecord(TOPIC_REQUEST, songId, timestamp.toString + hash.toString)
    val result = requestProducer.send(record)
  }

  def predictRequestWaitingConsumer(): Unit = {
    requestConsumer.subscribe(util.Collections.singletonList(TOPIC_REQUEST))
    import java.io._
    val processedMap = new HashMap[Int, Long]
    println("-----Consumer started")
    while(true){
      println("---------here2")
      val records=requestConsumer.poll(5000)
      println("---------poll")
      if (records.count() > 0) {
        println("Streaming: " + records.count())
        var textToWrite = ""
        for (record <- records.asScala) {
          val key = record.value().substring(10, record.value().length).toInt
          val keyTime = record.value().substring(0, 10).toLong

          if(!processedMap.contains(key)){
            val resList = predictExample(record.key())
            textToWrite += buildString(record.key(), resList)
            processedMap.put(key, keyTime)
          } else if(processedMap.contains(key) && processedMap.get(key).get <= keyTime){
            val resList = predictExample(record.key())
            textToWrite += buildString(record.key(), resList)
            processedMap.update(key, keyTime)
          }
        }
//        if(textToWrite.length > 0){
//          println("Recommendations:")
//          println(textToWrite)
//          val path = "target/tmp/"
//          val theDir = new File(path)
//          if (!theDir.exists()) theDir.mkdir()
//          val file = new File(path + "/" + Instant.now.getEpochSecond + ".txt" )
//          file.createNewFile()
//          val pw = new PrintWriter(file)
//          pw.write(textToWrite)
//          pw.close
//        }
      }
    }

    // combining kafka and spark to Direct Stream (ISSUE now)
    /*
   import org.apache.spark.streaming.kafka010._
   import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
   import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

   val kafkaParams = Map[String, Object](val path = "target/tmp/"
        val theDir = new File(path)
        if (!theDir.exists()) theDir.mkdir()
        val file = new File(path + "/" + Instant.now.getEpochSecond + ".txt" )
        file.createNewFile()
        val pw = new PrintWriter(file)
        pw.write(textToWrite)
        pw.closeval path = "target/tmp/"
        val theDir = new File(path)
        if (!theDir.exists()) theDir.mkdir()
        val file = new File(path + "/" + Instant.now.getEpochSecond + ".txt" )
        file.createNewFile()
        val pw = new PrintWriter(file)
        pw.write(textToWrite)
        pw.close
     "bootstrap.servers" -> kafka_server,
     "key.deserializer" -> classOf[StringDeserializer],
     "value.deserializer" -> classOf[StringDeserializer],
     "group.id" -> props.getProperty("group.id"),
     "auto.offset.reset" -> "latest",
     "enable.auto.commit" -> (true: java.lang.Boolean)
   )
   val topics = Array(TOPIC_REQUEST)
   val stream = KafkaUtils.createDirectStream[String, String](
     streamingContext,
     PreferConsistent,
     Subscribe[String, String](topics, kafkaParams)
   )

   while(true){
     Thread.sleep(2500)

     val dstream = stream.map(record => {
       println("Value: " + record.value())
       (record.value)
     })
     dstream.foreachRDD(rdd => {
       rdd.map(str => {
         println("Str: " + str)
       }).collect()
     })
   }
   */
  }

  def songRecommendationStreamProducer(songId: String, recList: String): Unit = {
    val record = new ProducerRecord(TOPIC_RECOMMENDATION, songId, recList)
    val result = requestProducer.send(record)
  }

  def resultWaitingConsumer(songId: String): Unit = {
    val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(util.Collections.singletonList(TOPIC_RECOMMENDATION))

    while(true){
      val records=consumer.poll(2000)
      if (records.count() > 0) {
        println(records.count())
        for (record <- records.asScala) {
          println(record.value())
          consumer.close()
        }
      }
    }
  }

  def predictStreaming(numSongs: Double): Unit = {
//    val path = getClass.getResource(streaming_source).getPath
//    val bufferedSource = Source.fromFile(

//    val readConfig = ReadConfig(Map("database" -> DB, "collection" -> resultsColl, "readPreference.name" -> "Primary"), Some(ReadConfig(sparkContext)))
//    val dataSet = MongoSpark.load(sparkContext, readConfig)
//    val percent = numSongs/dataSet.count()
//    val sampledStream = dataSet.sample(withReplacement = false, percent)
    val popularSongs = findPopular()
    val percent = numSongs/popularSongs.count()
    val sampledStream = popularSongs.sample(withReplacement = false, percent)
    println("Sampled tracks: " + sampledStream.count())
    val sampledArray = sampledStream.map(
      doc => {
        doc._1
      }
    ).collect()

    sampledArray.foreach(str => {
      songPredictStreamProducer(str, Instant.now.getEpochSecond, this.hashCode())
      Thread.sleep(1000)
    })

//    resultWaitingConsumer("")
//    for (line <- bufferedSource.getLines){
//      val cols = line.split(",").map(_.trim)
//      songPredictStreamProducer(cols(1))
//      Thread.sleep(1000)
//    }
//    bufferedSource.close()

  }


  private def printContinueMessage(): Unit = {
    println("Check your Spark web UI at http://localhost:4040 and press Enter to continue. [Press Backspace and Enter again if pressing enter once does not work]")
    scala.io.StdIn.readLine()
    println("Continuing, please wait....")
  }


}


