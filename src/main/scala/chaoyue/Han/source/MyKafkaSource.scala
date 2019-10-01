package chaoyue.Han.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object MyKafkaSource {
  val properties = new Properties
  properties.setProperty("bootstrap.servers", "hdp-dn-01:9092,hdp-dn-02:9092,hdp-dn-03:9092")
  properties.setProperty("group.id", "test11")
  def getKafkaSource(topic: String) = {
    val myconsumer = new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema, properties)
    myconsumer
  }
}
