package morgan.mu.flinkFromKafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import morgan.mu.sink.HbaseSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;


import java.util.Properties;

/**
 * @Title: FlinkFromKafkaToHbase
 * @Description: flink消费数据写入hbase
 * @Author: YuSong.Mu
 * @Date: 2019/8/26 11:08
 */
public class FlinkFromKafkaToHbase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(500);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "dev-bigdata01.aiads-host.com:9092,dev-bigdata02.aiads-host.com:9092,dev-bigdata03.aiads-host.com:9092");
        properties.setProperty("group.id", "test4444");

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>("spider_maoyan_box_dashboard",
                new SimpleStringSchema(), properties);


        DataStreamSource<String> stringDataStreamSource = env.addSource(myConsumer);

        stringDataStreamSource.map(new MapFunction<String, Tuple2<String,String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return Tuple2.of(jsonObject.getString("totalBoxUnitInfo"),
                        jsonObject.getString("splitTotalBox"));
            }
        }).writeUsingOutputFormat(new HbaseSink());


        env.execute("Flink Streaming JavaAPI From Kafka To Hbase");
    }
}
