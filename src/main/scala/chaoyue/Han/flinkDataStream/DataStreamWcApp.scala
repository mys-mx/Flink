package chaoyue.Han.flinkDataStream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
object DataStreamWcApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val socket: DataStream[String] = env.socketTextStream("hdp-dn-01", 9999)

    val result: DataStream[(String, Int)] = socket.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(_._1).sum(1)

    result.addSink(println(_))

    env.execute("flink stream word count demo")
  }
}
