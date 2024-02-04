import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

object KafkaProducer extends App {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", classOf[StringSerializer].getName)
  props.put("value.serializer", classOf[StringSerializer].getName)

  val producer = new KafkaProducer[String, String](props)

  val apiURL = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-carburants-fichier-instantane-test-ods-copie/records?select=id%2Cville%2Cdep_name%2Creg_name%2Ccom_name%2Cprix_nom%2Cprix_maj%2Cprix_valeur"

  try {
    while (true) {
      val response = scala.io.Source.fromURL(apiURL).mkString

      // Envoyer les données au topic Kafka
      val record = new ProducerRecord[String, String]("quickstart-events", response)
      val metadata: RecordMetadata = producer.send(record).get()

      println(s"Envoi réussi - Offset: ${metadata.offset()}, Partition: ${metadata.partition()}, Timestamp: ${metadata.timestamp()}")
      producer.send(record)

      producer.flush()
      Thread.sleep(1000)
    }
  } catch {
    case e: Exception =>
      e.printStackTrace()
  } finally {
    producer.close()
  }
}
