package nl.rug.sc.recommendalgo

import java.util.ArrayList

import scala.collection.JavaConversions._
import breeze.linalg._
import org.apache.spark.{HashPartitioner, SparkContext}
import org.bson.Document
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.broadcast._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vectors, DenseVector => SparkDV, Matrix => SparkM, SparseVector => SparkSV, Vector => SparkV}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, Vector => BV, _}
import breeze.linalg._
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD

//import breeze.numerics._
import scala.math._
import util.Random.shuffle
//spark-shell -i als.scala to run this code
//SPARK_SUBMIT_OPTS="-XX:MaxPermSize=4g" spark-shell -i als.scala


//Implementation of sec 14.3 Distributed Alternating least squares from stanford Distributed Algorithms and Optimization tutorial.

object userConverter {

  def convertIdToInt(rddToConvert: RDD[Row]): Map[String, Int] = {


    // Assign unique Long id for each userId
    val userIdToInt =
      rddToConvert.map(_.getAs[String]("user")).distinct().zipWithUniqueId()

    // Reverse mapping from generated id to original
    val reverseMapping1 =
      userIdToInt map { case (l, r) => (r, l) }

    // Depends on data size, maybe too big to keep
    // on single machine
    val map1 =
    userIdToInt.collect().toMap.mapValues(_.toInt).map(identity)

    //    println("=====")
    //    println(map1)

    map1
  }
}

object songConverter{
  def convertIdToInt(rddToConvert: RDD[Row]): Map[String, Int] = {

    val songIdToInt =
      rddToConvert.map(_.getAs[String]("song")).distinct().zipWithUniqueId()

    val reverseMapping2 =
      songIdToInt map { case (l, r) => (r, l) }

    val map2 =
      songIdToInt.collect().toMap.mapValues(_.toInt).map(identity)

    //    println("=====")
    //    println(map2)

    map2
  }
}

object ratingCreater{
  def createRatings (rddToConvert: RDD[Row]): RDD[Rating] = {
    val userIdToInt = userConverter.convertIdToInt(rddToConvert)
    val songIdToInt = songConverter.convertIdToInt(rddToConvert)

    // Transform to MLLib rating
    val ratings = rddToConvert.map { obj =>
      Rating(userIdToInt(obj.getAs[String]("user")),
        songIdToInt(obj.getAs[String]("song")), obj.getAs[String]("count").toDouble)
    }

    ratings

  }

  def createTriplet (rddToConvert: RDD[Document]): RDD[(String, String, String)] = {
    val ratings = rddToConvert.map { obj =>
      (obj.getString("user"),
        obj.getString("song"),
        obj.getString("count"))
    }
    ratings
  }
}

object predictor{
  def get_related(artist_factors: RDD[(String, DenseVector[Double])], artistid: String, N:Int = 10): Array[(String, Double, DenseVector[Double])] = {
    // fully normalize artist_factors, so can compare with only the dot product
    println("===item factor length: " + artist_factors.count())

    val targetVector = artist_factors.lookup(artistid).head
    val topRecommended = artist_factors.map { obj =>
      val dotProduct = (targetVector - obj._2)
      (obj._1, dotProduct dot dotProduct, obj._2)
    }.sortBy(_._2, true).collect().slice(0, 100)

//    val topRecommended = artist_factors.map { obj =>
//      val dotProduct = normalize(obj._2) dot normalize(targetVector)
//      (obj._1, dotProduct, obj._2)
//    }.sortBy(_._2, false).collect().slice(0, 100)

    println(artistid)
    println(targetVector)
    println(normalize(targetVector))

    topRecommended.foreach{ obj =>
      println(obj)
    }

    return topRecommended
  }
}

class Recommend {
  implicit def toBreeze(v: SparkV): BV[Double] = {
    /** Convert a spark.mllib.linalg vector into breeze.linalg.vector
      * We use Breeze library for any type of matrix operations because mllib local linear algebra package doesn't have any support for it (in Spark 1.4.1)
      * mllib toBreeze function already exists but is private to mllib scope
      *
      * Automatically choose the representation (dense or sparse) which use the less memory to be store
      */
    val nnz = v.numNonzeros
    if (1.5 * (nnz + 1.0) < v.size) new BSV(v.toSparse.indices, v.toSparse.values, v.size) else BV(v.toArray)

  }

  def test(myRdd: RDD[Document]): Unit = {
    val numPar = 10
    val fraction = 1f/numPar
    val weiArr = Array.fill[Double](numPar)(fraction)
    val allpartitions = myRdd.randomSplit(weiArr)

    val totalPlayCount = allpartitions.map{ singlePar =>
      singlePar.map{ doc =>
        doc.getString("count").toInt
      }.reduce(_ + _)
    }.sum

    println(totalPlayCount)
    println(totalPlayCount)
  }


  def train(sc: SparkContext, myRdd: RDD[Document], k: Int): RDD[(String, DenseVector[Double])] = {
    myRdd.repartition(600)
    myRdd.persist(StorageLevel.MEMORY_ONLY)
    //loads ratings from file
    val ratings = ratingCreater.createTriplet(myRdd)
//    val ratings = sc.textFile("hdfs://cshadoop1.utdallas.edu//user/bxm142230/input/ratings.dat").map(l => (l.split("::")(0),l.split("::")(1),l.split("::")(2)))

    // counts unique songs
    val itemCount = ratings.map(x => x._2).distinct.count

    // counts unique user
    val userCount = ratings.map(x => x._1).distinct.count

    // get distinct songs
    val items = ratings.map(x => x._2).distinct

    // get distinct user
    val users = ratings.map(x => x._1).distinct


    //create item latent vectors
    val itemMatrix = items.map(x => (x, DenseVector.zeros[Double](k))).persist(StorageLevel.MEMORY_ONLY)
    //Initialize the values to 0.5
    // generated a latent vector for each item using song id as key Array((song_id,densevector)) e.g (2,DenseVector(0.5, 0.5, 0.5, 0.5, 0.5)
    var myitemMatrix = itemMatrix.map(x => (x._1, x._2(0 to k - 1) := 0.5)).partitionBy(new HashPartitioner(10)).persist(StorageLevel.MEMORY_ONLY)

    //create user latent vectors
    val userMatrix = users.map(x => (x, DenseVector.zeros[Double](k))).persist(StorageLevel.MEMORY_ONLY)
    //Initialize the values to 0.5
    // generate latent vector for each user using user id as key Array((userid,densevector)) e.g (2,DenseVector(0.5, 0.5, 0.5, 0.5, 0.5)
    var myuserMatrix = userMatrix.map(x => (x._1, x._2(0 to k - 1) := 0.5)).partitionBy(new HashPartitioner(10)).persist(StorageLevel.MEMORY_ONLY)

    // group rating by items. Elements of type org.apache.spark.rdd.RDD[(String, (String, String))] (itemid,(userid,rating)) e.g  (1,(2,3))
    val ratingByItem = ratings.map(x => (x._2, (x._1, x._3))).persist(StorageLevel.MEMORY_ONLY)

    // group rating by user.  Elements of type org.apache.spark.rdd.RDD[(String, (String, String))] (userid,(item,rating)) e.g  (1,(3,5))
    val ratingByUser = ratings.map(x => (x._1, (x._2, x._3))).persist(StorageLevel.MEMORY_ONLY)


    var i = 0

    for (i <- 1 to 10) {
      // regularization factor which is lambda.
      val regfactor = 1.0
      val regMatrix = DenseMatrix.zeros[Double](k, k) //generate an diagonal matrix with dimension k by k
      //filling in the diagonal values for the reqularization matrix.
//      regMatrix(0, ::) := DenseVector(regfactor, 0, 0, 0, 0).t
//      regMatrix(1, ::) := DenseVector(0, regfactor, 0, 0, 0).t
//      regMatrix(2, ::) := DenseVector(0, 0, regfactor, 0, 0).t
//      regMatrix(3, ::) := DenseVector(0, 0, 0, regfactor, 0).t
//      regMatrix(4, ::) := DenseVector(0, 0, 0, 0, regfactor).t

      for (j <- 0 to k - 1){
        regMatrix(j, j) = regfactor
      }

      var songJoinedData = myitemMatrix.join(ratingByItem).persist(StorageLevel.MEMORY_ONLY)
      var tempUserMatrixRDD = songJoinedData.map(q => {
        var song = q._1
        var (user, rating) = q._2._2
        var tempDenseVector = q._2._1
        var tempTransposeVector = tempDenseVector.t
        var tempDenseMatrix = tempDenseVector * tempTransposeVector
        (user, tempDenseMatrix)
      }
      ).reduceByKey(_ + _).map(q => (q._1, inv(q._2 + regMatrix))).persist(StorageLevel.MEMORY_ONLY)

      var tempUserVectorRDD = songJoinedData.map(q => {
        var song = q._1
        var (user, rating) = q._2._2
        var tempDenseVector = q._2._1

        var secondTempVector = tempDenseVector :* rating.toDouble
        (user, secondTempVector)
      }
      ).reduceByKey(_ + _).persist(StorageLevel.MEMORY_ONLY)

      var finalUserVectorRDD = tempUserMatrixRDD.join(tempUserVectorRDD).map(p => (p._1, p._2._1 * p._2._2)).persist(StorageLevel.MEMORY_ONLY)
      myuserMatrix = finalUserVectorRDD.partitionBy(new HashPartitioner(10)).persist(StorageLevel.MEMORY_ONLY)

      var userJoinedData = myuserMatrix.join(ratingByUser).persist(StorageLevel.MEMORY_ONLY)
      var tempSongMatrixRDD = userJoinedData.map(q => {
        var tempUser = q._1
        var (song, rating) = q._2._2
        var tempDenseVector = q._2._1
        var tempTransposeVector = tempDenseVector.t
        var tempDenseMatrix = tempDenseVector * tempTransposeVector
        (song, tempDenseMatrix)
      }
      ).reduceByKey(_ + _).map(q => (q._1, inv(q._2 + regMatrix))).persist(StorageLevel.MEMORY_ONLY)

      var tempSongVectorRDD = userJoinedData.map(q => {
        var tempUser = q._1
        var (song, rating) = q._2._2
        var tempDenseVector = q._2._1

        var secondTempVector = tempDenseVector :* rating.toDouble
        (song, secondTempVector)
      }
      ).reduceByKey(_ + _).persist(StorageLevel.MEMORY_ONLY)

      var finalSongVectorRDD = tempSongMatrixRDD.join(tempSongVectorRDD).map(p => (p._1, p._2._1 * p._2._2)).persist(StorageLevel.MEMORY_ONLY)
      myitemMatrix = finalSongVectorRDD.partitionBy(new HashPartitioner(10)).persist(StorageLevel.MEMORY_ONLY)

    }

    /* this helps check the approximation of SVD
    var pairsList = List(("fffff67d54a40927c93d03bd6c816b034b59f087", "SOEYEOG12A6D4FD103"),
      ("ffffdc274ca76d154b4e56b2dbc82ff538c93c0b", "SOXIGHW12A6D4F7245"),
      ("fffe00b418e708c7003ff284586248f264c04c17", "SORCOGI12A6310DB7F"))
    for ((user, song) <- pairsList) {
      var userDenseVector = myuserMatrix.lookup(user)(0)
      var songDenseVector = myitemMatrix.lookup(song)(0)
      var predictedRating = userDenseVector.t * songDenseVector
      println("==========================================================")
      println("Latent vector for user " + user + " : " + userDenseVector)
      println("Latent vector for song " + song + " : " + songDenseVector)
      println("Predicted Rating by user " + user + " for song " + song + " : " + predictedRating)
    }
//    */

    ratings.top(3).foreach(obj => {
      println(obj._1 + ", " + obj._2 + ", " + obj._3)
    })

    return myitemMatrix
  }

  def predictBySongId(songId: String, customRdd: MongoRDD[Document], k: Int): Array[(String, Double, DenseVector[Double])] = {

    println(customRdd.count())

    val temp =  customRdd.map{ doc =>
      val tp = doc.get("vectors").asInstanceOf[ArrayList[Double]]
      val denseVec = BDV.zeros[Double](k)
      for (j <- 0 to k - 1){
        denseVec(j) = tp.get(j)
      }
      (doc.getString("_id"), denseVec)
    }
    val mostSimilarSongs = predictor.get_related(temp, songId)
    return mostSimilarSongs
  }

}
