package com.aus234.producer

import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util._

import com.google.gson.{JsonObject, Gson}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import java.nio.charset.Charset;
import org.apache.kafka.clients.producer._

object TrasactionProducer {

  var applicationConf:Config = _
  val props = new Properties()
  var topic:String =  _
  var producer:KafkaProducer[String, String] = _

  def load = {

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, "3")
    topic = "creditcardTransaction"

  }

  def getCsvIterator(fileName:String) = {

    val file = new File(fileName)
    val csvParser = CSVParser.parse(file, Charset.forName("UTF-8"), CSVFormat.DEFAULT)
    csvParser.iterator()
  }


  def publishJsonMsg(fileName:String) = {
    val gson: Gson = new Gson
    val csvIterator = getCsvIterator(fileName)
    val rand: Random = new Random
    var count = 0

    while (csvIterator.hasNext) {
      val record = csvIterator.next()

      val obj: JsonObject = new JsonObject
      val isoFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      isoFormat.setTimeZone(TimeZone.getTimeZone("IST"));
      val d = new Date()
      val timestamp = isoFormat.format(d)
      val unix_time = d.getTime


      //println("Transaction Details:" + record.get(0),record.get(1),record.get(2),record.get(3),timestamp, record.get(7),record.get(8),record.get(9), record.get(10), record.get(11))

      obj.addProperty(TransactionKafkaEnum.cc_num, record.get(0))
      obj.addProperty(TransactionKafkaEnum.first, record.get(1))
      obj.addProperty(TransactionKafkaEnum.last, record.get(2))
      obj.addProperty(TransactionKafkaEnum.trans_num, record.get(3))
      obj.addProperty(TransactionKafkaEnum.trans_time, timestamp)
      //obj.addProperty(TransactionKafkaEnum.unix_time, unix_time)
      obj.addProperty(TransactionKafkaEnum.category, record.get(7))
      obj.addProperty(TransactionKafkaEnum.merchant, record.get(8))
      obj.addProperty(TransactionKafkaEnum.amt, record.get(9))
      obj.addProperty(TransactionKafkaEnum.merch_lat, record.get(10))
      obj.addProperty(TransactionKafkaEnum.merch_long, record.get(11))
      val json: String = gson.toJson(obj)
      println("Transaction Record: " + json)
      val producerRecord = new ProducerRecord[String, String](topic, json) //Round Robin Partitioner
       producer.send(producerRecord, new MyProducerCallback) /*Asynchrounous Produer */
      Thread.sleep(rand.nextInt(3000 - 1000) + 1000)
    }
  }

  class MyProducerCallback extends Callback {
    def onCompletion(recordMetadata: RecordMetadata, e: Exception) {
      if (e != null) System.out.println("AsynchronousProducer failed with an exception" + e)
      else {
        System.out.println("Sent data to partition: " + recordMetadata.partition + " and offset: " + recordMetadata.offset)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    load
    producer = new KafkaProducer[String, String](props)
    val file = "src/main/resources/transactions.csv"
    publishJsonMsg(file)
  }
}
