package morgan.mu.flinkFromKafka;

/**
 * @Title: FilnkCostKafka
 * @Description: flink测试连接kafka
 * @Author: YuSong.Mu
 * @Date: 2019/5/15 10:01
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSource;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FilnkCostKafka {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //默认情况下，检查点被禁用。要启用检查点，请在StreamExecutionEnvironment上调用enableCheckpointing(n)方法，
        // 其中n是以毫秒为单位的检查点间隔。每隔5000 ms进行启动一个检查点,则下一个检查点将在上一个检查点完成后5秒钟内启动

        env.enableCheckpointing(500);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        env.setStartFromEarliest();
        Properties properties = new Properties();
        //kafka的节点的IP或者hostName，多个使用逗号分隔
        properties.setProperty("bootstrap.servers", "hdp-dn-01:9092,hdp-dn-02:9092,hdp-dn-03:9092");
        //zookeeper的节点的IP或者hostName，多个使用逗号进行分隔
        properties.setProperty("group.id", "test");
        System.out.println("11111111111");
        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("test",
                new SimpleStringSchema(), properties);


//        getKafkaConsumerOffset(myConsumer);

        DataStream<String> keyedStream = env.addSource(myConsumer);
        System.out.println("2222222222222");


        keyedStream.map(new MapFunction<String, Map<String,Object>>() {
            @Override
            public Map<String, Object> map(String json) throws Exception {
                Map<String, Object> stringObjectHashMap = new HashMap<>();
                JSONObject jsonObject = JSON.parseObject(json);
                stringObjectHashMap.put("name",jsonObject.getString("name"));
                stringObjectHashMap.put("age",jsonObject.getInteger("age"));
                stringObjectHashMap.put("school",jsonObject.getString("school"));
                stringObjectHashMap.put("subject",jsonObject.getString("subject"));
                return stringObjectHashMap;
            }
        }).print();


        System.out.println("3333333333333");
        env.execute("Flink Streaming Java API Skeleton");
    }

/**
 * 上面的示例将使用者配置为从主题的分区0,1和2的指定偏移量开始myTopic。偏移值应该是消费者应为每个分区读取的下一条记录。
 * 注意 1：如果使用者需要读取在提供的偏移量映射中没有指定偏移量的分区，则它将回退到setStartFromGroupOffsets()该特定分区的默认组偏移行为。
 * 注意 2：当作业从故障中自动恢复或使用保存点手动恢复时，这些起始位置配置方法不会影响起始位置。
 * 在恢复时，每个Kafka分区的起始位置由存储在保存点或检查点中的偏移量确
 */
    /**
     * 从topic的指定分区的指定偏移量开始消费
     */
    private static void getKafkaConsumerOffset(FlinkKafkaConsumer010<String> myConsumer) {
        Map<KafkaTopicPartition, Long> kafkaTopicPartitionLongHashMap = new HashMap<>();

        kafkaTopicPartitionLongHashMap.put(new KafkaTopicPartition("", 0), 10L);
        kafkaTopicPartitionLongHashMap.put(new KafkaTopicPartition("", 0), 12L);
        kafkaTopicPartitionLongHashMap.put(new KafkaTopicPartition("", 0), 15L);
        myConsumer.setStartFromSpecificOffsets(kafkaTopicPartitionLongHashMap);
    }


    /**
     * 逻辑处理部分
     */
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            //将kafka中的单词全部编程小写，并根据非单词字符切分
            String[] tokens = value.toLowerCase().split("\\W+");
            //将数据转换成元组类型：（字符串,1）
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }


}
