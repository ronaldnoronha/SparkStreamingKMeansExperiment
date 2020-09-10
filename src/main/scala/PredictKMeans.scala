package example.stream

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext.jarOfObject

import scala.io.Source
import java.time.LocalDateTime
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeansModel, StreamingKMeansModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.Array.range
import scala.collection.immutable.ListMap
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

//import org.pmml4s.model.Model

object PredictKMeans {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingKMeansModelTraining")
    val ssc = new StreamingContext(conf,Seconds(1))

    println("Starting Training at "+ LocalDateTime.now())

    // read the random centers
    val centers:Array[linalg.Vector] = new Array[linalg.Vector](8)
    for (i <- 0 to centers.length-1) {
      centers(i) = Vectors.dense(Array(Random.nextDouble, Random.nextDouble, Random.nextDouble).map(_*30-15))
    }

    // create equal weights
    val weights:Array[Double] = new Array[Double](centers.length)
    for (i<-0 to weights.length-1) {
      weights(i) = 1/centers.length
    }

    // Get real centers
    val filename = "/home/ronald/centers.csv"
    val lines = Source.fromFile(filename).getLines.toArray.map(_.drop(1).dropRight(1)).map(_.split(","))

    val realCenters: Array[linalg.Vector] = new Array[linalg.Vector](lines.length - 1)
    for (i <- 1 to lines.length - 1) {
      realCenters(i - 1) = Vectors.dense(lines(i).map(_.toDouble))
    }

    val model = new StreamingKMeansModel(centers,weights)
    val count = ssc.sparkContext.longAccumulator("Counter")
    val correct = ssc.sparkContext.longAccumulator("Correct")
    val emptyRDD = ssc.sparkContext.longAccumulator("Empty RDDs")

    val brokers = args(0)
    val groupId = args(1)
    val topics = args(2)

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)).map(_.value).map(_.stripPrefix("\"").trim).map(_.stripSuffix("\"").trim)

    val inputLines = messages.map(_.split(","))

    // train model
    inputLines.foreachRDD(rdd => {
      if (rdd.isEmpty()) {
        emptyRDD.add(1)
      } else {
        emptyRDD.reset()
      }
//      if (emptyRDD.value>150L) {
//        ssc.stop()
//      }
      val points = rdd.map(_(1).split(" ").map(_.toDouble)).map(x=>Vectors.dense(x))
      model.update(points, 1.0, "batches")
      val modelCenters = model.clusterCenters
      val reference = checkPrediction(realCenters, modelCenters)
      for (i <- rdd) {
        count.add(1)
        val prediction = model.predict(Vectors.dense(i(1).split(" ").map(_.toDouble)))
        val target = i(2).toInt
        if (prediction==reference(target)) {
          correct.add(1)
        }
      }
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(750000)
    println("Streaming stopped at "+LocalDateTime.now())
    ssc.stop()

    println("Number of messages: "+ count.value)
    println("Number of correct predictions: "+ correct.value)
    println("Number of empty RDDs: "+ emptyRDD.value)
  }

  def checkPrediction(realCenters:Array[linalg.Vector], modelCenters:Array[linalg.Vector]) : Array[Int] = {
    val length = realCenters.length

    // Create distTable
    val distMap = scala.collection.mutable.Map[String, Double]()
    for (i <- 0 to length*length-1) {
      distMap +=  ((i/length).toString+(i%length).toString ->
        Vectors.sqdist(realCenters(i/length), modelCenters(i%length)))
    }

    val result = Array.fill[Int](length)(-1)
    val set = scala.collection.mutable.Set(range(0,length):_*)

    breakable {
      ListMap(distMap.toSeq.sortBy(_._2):_*).foreach{
        case (key, value) => {
          if (result(key(0).asDigit).==(-1) & set.exists(y=>key(1).asDigit==y)) {
            set-=key(1).asDigit
            result(key(0).asDigit)=key(1).asDigit
          }
        }
          if (set.isEmpty) break
      }
    }
    return result
  }
}
