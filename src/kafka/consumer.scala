import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, ConsumerRecords}
import scala.collection.JavaConverters._

object KafkaConsumerExample extends App {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "carburant_api")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(Seq("quickstart-events").asJava)

  try {
    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(100)

      for (record <- records.asScala) {
        println(s"Reçu: ${record.value}")
      }
    }
  } catch {
    case e: Exception =>
      e.printStackTrace()
  } finally {
    consumer.close()
  }
}
