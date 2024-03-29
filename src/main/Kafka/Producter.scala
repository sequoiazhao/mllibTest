package Kafka

import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

/**
  * @author zhaoming on 2019-05-23 16:57
  **/
object Producter {
  def main(args: Array[String]): Unit = {

    val events = args(0).toInt
    val topic = args(1)
    val brokers = args(2)

    val rnd = new Random()

    val props = new Properties()

    props.put("bootstrap.servers", brokers)
    props.put("client.id", "ScalaProducerExample")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val t = System.currentTimeMillis()

    for (nEvents <- Range(0, events)) {
      val runtime = new Date().getTime()
      val ip = "10.18.216." + rnd.nextInt(255)
      val msg = runtime + "," + nEvents + ",www.example.com," + ip
      val data = new ProducerRecord[String, String](topic, ip, msg)

      //async
      //producer.send(data, (m,e) => {})
      //sync
      producer.send(data)

    }
    System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
    producer.close()


  }
}
