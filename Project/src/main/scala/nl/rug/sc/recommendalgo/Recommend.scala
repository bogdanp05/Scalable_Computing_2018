package nl.rug.sc.recommendalgo

import breeze.linalg._
import org.apache.spark.{HashPartitioner, SparkContext}
import org.bson.Document
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.broadcast._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vectors, DenseVector => SparkDV, Matrix => SparkM, SparseVector => SparkSV, Vector => SparkV}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, Vector => BV, _}
import breeze.linalg._

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

  def createTriplet (rddToConvert: RDD[Row]): RDD[(String, String, String)] = {
    val ratings = rddToConvert.map { obj =>
      (obj.getAs[String]("user"),
        obj.getAs[String]("song"),
        obj.getAs[String]("count"))
    }
    ratings
  }
}

object predictor{
  def get_related(artist_factors: RDD[(String, DenseVector[Double])], artistid: String, N:Int = 10) {
    // fully normalize artist_factors, so can compare with only the dot product

    artist_factors.map { obj =>


    }
    norm.scalarNorm_Double(artist_factors)
    normalize.normalizeDoubleImpl
      norms = numpy.linalg.norm(artist_factors, axis =-1)
    self.factors = artist_factors / norms[:    , numpy.newaxis    ]

    scores = self.factors.dot(self.factors[artistid])
    best = numpy.argpartition(scores, -N)[- N:    ]
    return sorted(zip(best, scores[best]), key = lambda x: - x[1 ] )
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

  def train(sc: SparkContext, myRdd: RDD[Row]): Unit = {

    //loads ratings from file
    val ratings = ratingCreater.createTriplet(myRdd)
//    val ratings = sc.textFile("hdfs://cshadoop1.utdallas.edu//user/bxm142230/input/ratings.dat").map(l => (l.split("::")(0),l.split("::")(1),l.split("::")(2)))

    // counts unique movies
    val itemCount = ratings.map(x => x._2).distinct.count

    // counts unique user
    val userCount = ratings.map(x => x._1).distinct.count

    // get distinct movies
    val items = ratings.map(x => x._2).distinct

    // get distinct user
    val users = ratings.map(x => x._1).distinct

    // latent factor
    val k = 5

    //create item latent vectors
    val itemMatrix = items.map(x => (x, DenseVector.zeros[Double](k)))
    //Initialize the values to 0.5
    // generated a latent vector for each item using movie id as key Array((movie_id,densevector)) e.g (2,DenseVector(0.5, 0.5, 0.5, 0.5, 0.5)
    var myitemMatrix = itemMatrix.map(x => (x._1, x._2(0 to k - 1) := 0.5)).partitionBy(new HashPartitioner(10)).persist

    //create user latent vectors
    val userMatrix = users.map(x => (x, DenseVector.zeros[Double](k)))
    //Initialize the values to 0.5
    // generate latent vector for each user using user id as key Array((userid,densevector)) e.g (2,DenseVector(0.5, 0.5, 0.5, 0.5, 0.5)
    var myuserMatrix = userMatrix.map(x => (x._1, x._2(0 to k - 1) := 0.5)).partitionBy(new HashPartitioner(10)).persist

    // group rating by items. Elements of type org.apache.spark.rdd.RDD[(String, (String, String))] (itemid,(userid,rating)) e.g  (1,(2,3))
    val ratingByItem = ratings.map(x => (x._2, (x._1, x._3)))

    // group rating by user.  Elements of type org.apache.spark.rdd.RDD[(String, (String, String))] (userid,(item,rating)) e.g  (1,(3,5))
    val ratingByUser = ratings.map(x => (x._1, (x._2, x._3)))


    var i = 0
    for (i <- 1 to 10) {
      // regularization factor which is lambda.
      val regfactor = 1.0
      val regMatrix = DenseMatrix.zeros[Double](k, k) //generate an diagonal matrix with dimension k by k
      //filling in the diagonal values for the reqularization matrix.
      regMatrix(0, ::) := DenseVector(regfactor, 0, 0, 0, 0).t
      regMatrix(1, ::) := DenseVector(0, regfactor, 0, 0, 0).t
      regMatrix(2, ::) := DenseVector(0, 0, regfactor, 0, 0).t
      regMatrix(3, ::) := DenseVector(0, 0, 0, regfactor, 0).t
      regMatrix(4, ::) := DenseVector(0, 0, 0, 0, regfactor).t

      var movieJoinedData = myitemMatrix.join(ratingByItem)
      var tempUserMatrixRDD = movieJoinedData.map(q => {
        var movie = q._1
        var (user, rating) = q._2._2
        var tempDenseVector = q._2._1
        var tempTransposeVector = tempDenseVector.t
        var tempDenseMatrix = tempDenseVector * tempTransposeVector
        (user, tempDenseMatrix)
      }
      ).reduceByKey(_ + _).map(q => (q._1, inv(q._2 + regMatrix)))

      var tempUserVectorRDD = movieJoinedData.map(q => {
        var movie = q._1
        var (user, rating) = q._2._2
        var tempDenseVector = q._2._1

        var secondTempVector = tempDenseVector :* rating.toDouble
        (user, secondTempVector)
      }
      ).reduceByKey(_ + _)

      var finalUserVectorRDD = tempUserMatrixRDD.join(tempUserVectorRDD).map(p => (p._1, p._2._1 * p._2._2))
      myuserMatrix = finalUserVectorRDD.partitionBy(new HashPartitioner(10)).persist

      var userJoinedData = myuserMatrix.join(ratingByUser)
      var tempMovieMatrixRDD = userJoinedData.map(q => {
        var tempUser = q._1
        var (movie, rating) = q._2._2
        var tempDenseVector = q._2._1
        var tempTransposeVector = tempDenseVector.t
        var tempDenseMatrix = tempDenseVector * tempTransposeVector
        (movie, tempDenseMatrix)
      }
      ).reduceByKey(_ + _).map(q => (q._1, inv(q._2 + regMatrix)))

      var tempMovieVectorRDD = userJoinedData.map(q => {
        var tempUser = q._1
        var (movie, rating) = q._2._2
        var tempDenseVector = q._2._1

        var secondTempVector = tempDenseVector :* rating.toDouble
        (movie, secondTempVector)
      }
      ).reduceByKey(_ + _)

      var finalMovieVectorRDD = tempMovieMatrixRDD.join(tempMovieVectorRDD).map(p => (p._1, p._2._1 * p._2._2))
      myitemMatrix = finalMovieVectorRDD.partitionBy(new HashPartitioner(10)).persist

    }

    /*
    user 1 and movieid 914,
    user 1757 and movieid 1777,
    user 1759 and movieid 231.
    */


    var pairsList = List(("f84f5b5a5c5d1d9fb4866f6488e0d2661b54c192", "SOZNUNA12A6701FCC7"),
      ("f84f5b5a5c5d1d9fb4866f6488e0d2661b54c192", "SOXFIHR12AB017BAF8"),
      ("f84f5b5a5c5d1d9fb4866f6488e0d2661b54c192", "SOWKQYL12AB0183B15"))
    for ((user, movie) <- pairsList) {
      var userDenseVector = myuserMatrix.lookup(user)(0)
      var movieDenseVector = myitemMatrix.lookup(movie)(0)
      var predictedRating = userDenseVector.t * movieDenseVector
      println("==========================================================")
      println("Latent vector for user " + user + " : " + userDenseVector)
      println("Latent vector for movie " + movie + " : " + movieDenseVector)
      println("Predicted Rating by user " + user + " for movie " + movie + " : " + predictedRating)
    }

    ratings.top(3).foreach(obj => {
      println(obj._1 + ", " + obj._2 + ", " + obj._3)
    })

    predictor.get_related(myitemMatrix, "SOWKQYL12AB0183B15")
  }

}
