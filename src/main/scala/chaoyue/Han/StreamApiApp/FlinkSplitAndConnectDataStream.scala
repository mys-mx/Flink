package chaoyue.Han.StreamApiApp

import chaoyue.Han.source.MyKafkaSource
import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.api.scala._


/**
  * flink 分流操作  和多流join操作
  */
object FlinkSplitDataStream {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val dataStream: DataStream[String] = env.addSource(MyKafkaSource.getKafkaSource("log"))


    //将json字符串转成 对象
    val tuple: DataStream[StrTuple] = dataStream.map(json => JSON.parseObject(json, classOf[StrTuple]))

    //根据标签进行切分
    val splitStream = tuple.split(startuplog => {
      var flag: List[String] = null
      if ("LiMing" == startuplog.name) {

        flag = List("xiaopengyou", "xiaoxuesheng")
      } else {
        flag = List("daxuesheng")
      }
      flag
    })
    val result: DataStream[StrTuple] = splitStream.select("xiaoxuesheng")
    val result1: DataStream[StrTuple] = splitStream.select("daxuesheng")

    //result.print("xiaoxuesheng")
    //result1.print("daxuesheng")

    /**
      * 双流join
      */
    /*val connStream: ConnectedStreams[StrTuple, StrTuple] = result.connect(result1)

    connStream.map(
      (strTuplelog:StrTuple)=>strTuplelog.name,
      (strTuplelog1:StrTuple)=>strTuplelog1.name
    ).print("join")
*/
    /**
      * 多流join
      */

    result.union(result1).print("sum join")

    env.execute("flink stream")
  }

}
//{"id":"1","name":"LiMing","age":17,"school":"北京大学","subject":"bigdata","time
case class StrTuple(id: String, name: String, age: Int, school: String, subject: String, time: Long)

