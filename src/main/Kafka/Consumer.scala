package Kafka

import java.util.concurrent.{ExecutorService, Executors}
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}

/**
  * @author zhaoming on 2019-05-23 18:41
  **/
object Consumer {


  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def main(args: Array[String]): Unit = {

    val groupId = args(0)
    val topic = args(1)
    val brokers = args(2)

    val props = createConsumerConfig(brokers, groupId)
    val consumer = new KafkaConsumer[String, String](props)
    val executor: ExecutorService = null

    createConsumerConfig(brokers, groupId)
    consumer.subscribe(Collections.singletonList(topic))

    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val records = consumer.poll(1000)

          println(records.count)
          import scala.collection.JavaConversions._

          if (!records.isEmpty) {
            for (record: ConsumerRecord[String, String] <- records) {
              println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
            }
          }


        }
      }
    })

    //    if (consumer != null)
    //      consumer.close()
    //    if (executor != null)
    //      executor.shutdown()

  }

}
