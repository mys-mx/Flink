package chaoyue.Han.flinkDataStream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

object DataStreamWcApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //事件发生时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val socket: DataStream[String] = env.socketTextStream("hdp-dn-01", 9999)


    val result = socket
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
        override def extractTimestamp(element: String): Long = element.split(" ")(0).toLong
      })
    result.map(t => (t.split(" ")(1), 1)).keyBy(0)
      .timeWindow(Time.seconds(2), Time.seconds(1))
      .sum(1).print()




    //    result.print()

    env.execute("flink stream word count demo")
  }
}
