package chaoyue.Han.flinkDataSet

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.elasticsearch.search.aggregations.InternalOrder.Aggregation
//必须导入的隐式转换
import org.apache.flink.api.scala._

object FlinkWC {
  def main(args: Array[String]): Unit = {

    //获取flink env
    val env = ExecutionEnvironment.getExecutionEnvironment

    val textDataSet = env.readTextFile("E:\\a.json")

    val result = textDataSet.flatMap(_.split(","))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    result.collect().sortBy(-_._2).foreach(println(_))


    //    result.print()


  }

}
