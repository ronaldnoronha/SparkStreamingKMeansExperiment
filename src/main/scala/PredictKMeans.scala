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




    val conf = new SparkConf().setAppName("StreamingKMeansModelTraining")
    val sc = new SparkContext(conf)

    val model = KMeansModel.load(sc, "/home/ronald/kmeansModel")
    val centers = model.clusterCenters
    val weights:Array[Double] = new Array[Double](centers.length)
    for (i<-0 to weights.length-1) {
      weights(i) = 1/centers.length
    }
    for (i<-centers){
      println(i.toString())
    }
    sc.stop()



//    val ssc = new StreamingContext(conf, Seconds(1))
//    val streamingModel = new StreamingKMeansModel(centers,weights)
//
//    val brokers = args(0)
//    val groupId = args(1)
//    val topics = args(2)
//
//    val topicsSet = topics.split(",").toSet
//    val kafkaParams = Map[String, Object](
//      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
//      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
//      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
//      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
//    val messages = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
//
//    val strippedMessages = messages.map(_.value).map(_.split("\""))
//    val inputLines = strippedMessages.map(_(1).split(","))
//
//    inputLines.foreachRDD( rdd => {
//      for (i <- rdd) {
//        println(i(0)+" "+"Prediction: "+ streamingModel.predict(Vectors.dense(i(1).split(" ").map(_.toDouble)))+
//        " Target: "+i(2)+" "+LocalDateTime.now().toString())\
//      }
//    })
//
//    ssc.start()
//    ssc.awaitTerminationOrTimeout(120000)
//    ssc.stop()


  }
}
