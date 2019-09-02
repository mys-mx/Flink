package chaoyue.Han.flinkDataSet

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
//必须导入的隐式转换
import org.apache.flink.api.scala._

object FlinkWC {
  def main(args: Array[String]): Unit = {

    //获取flink env
    val env = ExecutionEnvironment.getExecutionEnvironment

    val textDataSet = env.readTextFile("E:\\a.json")

    textDataSet.flatMap(_.split(","))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()


  }

}
