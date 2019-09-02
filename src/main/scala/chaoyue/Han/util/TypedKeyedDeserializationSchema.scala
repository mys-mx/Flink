package chaoyue.Han.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
case class KafkaMsg(key: String, value: String, topic: String, partiton: Int, offset: Long)
class TypedKeyedDeserializationSchema extends KeyedDeserializationSchema[KafkaMsg] {
  def deserialize(key: Array[Byte],
                  value: Array[Byte],
                  topic: String,
                  partition: Int,
                  offset: Long
                 ): KafkaMsg =
    KafkaMsg(new String(key),
      new String(value),
      topic,
      partition,
      offset
    )

  def isEndOfStream(e: KafkaMsg): Boolean = false

  def getProducedType(): TypeInformation[KafkaMsg] = createTypeInformation
}