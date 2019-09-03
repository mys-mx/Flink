package chaoyue.Han.flinkDataSet

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.elasticsearch.search.aggregations.InternalOrder.Aggregation
//必须导入的隐式转换
import org.apache.flink.api.scala._

object FlinkWC {
  def main(args: Array[String]): Unit = {

    val tool = ParameterTool.fromArgs(args)
    val inputPath: String = tool.get("input")
    val outputPath: String = tool.get("output")


    //获取flink env
    val env = ExecutionEnvironment.getExecutionEnvironment

    val textDataSet = env.readTextFile(inputPath)

    val result = textDataSet.flatMap(_.split(","))
      .map((_, 1))
      .groupBy(0)
      .sum(1)


    result.setParallelism(1)
      .sortPartition(1,Order.DESCENDING).first(10).writeAsCsv(outputPath)

    env.execute()
  }

}
