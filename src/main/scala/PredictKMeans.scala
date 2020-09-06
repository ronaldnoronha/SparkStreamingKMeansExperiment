package example.stream

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.io.Source
import java.time.{LocalDateTime, Duration}
import java.lang.Long
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
    println("Started at "+LocalDateTime.now().toString())
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

    val count = ssc.sparkContext.longAccumulator("Counter")
    val correct = ssc.sparkContext.longAccumulator("Correct Prediction")
    val time = ssc.sparkContext.collectionAccumulator[Double]("Creation time")

    val lineMessages = messages.map(_.split(","))
    lineMessages.foreachRDD( rdd => {
      for (i <- rdd) {
        count.add(1)
        val point = streamingModel.predict(Vectors.dense(i(1).split(" ").map(_.toDouble)))
        if (point==i(2).toInt) {
          correct.add(1)
        }

        time.add(Duration.between(LocalDateTime.parse(i(0).split(" ").mkString("T")),LocalDateTime.now()).toNanos().toDouble)
      }
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(45000)

    println("Total number of messages received: " + count.value)
    println("Total correct predictions: "+correct.value)

//    println(time.value.size())
//    var sum = 0.toDouble
//    for (i<-time.value.toArray()){
////      println(i.getClass)
//    }
//    println(sum/time.value.size(
    //    ))
    println("Finished at "+LocalDateTime.now().toString())
  }
}
