package example.stream

import java.time.LocalDateTime

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.io.Source
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeansModel, StreamingKMeansModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//import org.pmml4s.model.Model

object PredictKMeans {
  def main(args: Array[String]): Unit = {
    val filename = "/home/ronald/kmeansModel"
    val lines = Source.fromFile(filename).getLines.toArray.map(_.drop(1).dropRight(1)).map(_.split(","))

    val centers:Array[linalg.Vector] = new Array[linalg.Vector](lines.length)
    for (i <- 0 to lines.length-1) {
      centers(i) = Vectors.dense(lines(i).map(_.toDouble))
    }

    val weights:Array[Double] = new Array[Double](centers.length)
    for (i<-0 to weights.length-1) {
      weights(i) = 1/centers.length
    }

    val conf = new SparkConf().setAppName("StreamingKMeansModelPredicting")
    val ssc = new StreamingContext(conf, Seconds(1))
    val streamingModel = new StreamingKMeansModel(centers,weights)

    for (i<-streamingModel.clusterCenters) {
      println(i)
    }

//    val broadcastModel = ssc.broadcast(streamingModel)

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

    val lineMessages = messages.map(_.split(","))

    lineMessages.foreachRDD(rdd=> {
      for (i <- rdd) {
        println(i(0)+" "+"Prediction: "+streamingModel.predict(Vectors.dense(i(1).split(" ").map(_.toDouble)))+
          " Target: "+i(2)+" "+LocalDateTime.now().toString())
      }
    })


//    val time = lineMessages.map(_(0)+" ")
//    val prediction = lineMessages.map(_(1).split(" ").map(_.toDouble)).map(Vectors.dense(_)).map(streamingModel.predict(_))
//    val target = lineMessages.map(_(2))
//    val result = time.union(prediction.map(_.toString()))

//    val time = lineMessages.map(_(0)+ " " + LocalDateTime.now().toString())
//    val prediction = rdd.map(_(1).split(" ").map(_.toDouble)).map(Vectors.dense(_)).map(streamingModel.predict(_))
//
//    val result = lineMessages.transform{ rdd =>
//      rdd.map(_(0)+" Prediction: "+(_(1).split(" ").map(_.toDouble))
//        .map(Vectors.dense(_))
//        .map(streamingModel.predict(_)))
//    }
//    val result = lineMessages.map(_(0)+" Prediction: "+
//      _(1).split(" ").map(_.toDouble)

//    val point = lineMessages.map(_(0)+" "+_(1))

//    +(rdd(1).split(" ").map(_.toDouble)).map(Vectors.dense(_)).map(streamingModel.predict(_))
//    val point = lineMessages.map(_(0)+" "+_(1).split(" ").map(_.toDouble))

//    val result = lineMessages()
//    +lineMessages.map(_(1)+" ")
    // test 2
//    time.print()
//    result.print()

    // test 3
//    result.print()

//    val result = lineMessages

//        val strippedMessages = messages.map(_.value).map(_.split("\""))
//    strippedMessages.foreachRDD( rdd => {
//      for (i <- rdd) {
//        for (j <- i) {
//          println(j)
//        }
//      }
//    })
//    val inputLines = strippedMessages.map(_(1).split(","))
//
//    inputLines.foreachRDD( rdd => {
//      for (i <- rdd) {
//        //        for (j <- i){
//        //          println(j)
//        //        }
//        val point = Vectors.dense(i(1).split(" ").map(_.toDouble))
//        println(point.toJson)
//        //        println(i(0)+" "+"Prediction: "+streamingModel.predict(point)+ " Target: "+i(2)+" "+LocalDateTime.now().toString())
//      }
//    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(60000)

  }
}
