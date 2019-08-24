package morgan.mu.flinkFromKafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import morgan.mu.sink.MongoDBsink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;


import java.util.Properties;

/**
 * @Title: FlinkFromKafkaToMongodb
 * @Description: flink消费kafka数据到mongodb
 * @Author: YuSong.Mu
 * @Date: 2019/8/22 16:04
 */
public class FlinkFromKafkaToMongodb {
    public static void main(String[] args) throws Exception {
        //flink启动命令
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(500);


        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "dev-bigdata01.aiads-host.com:9092,dev-bigdata02.aiads-host.com:9092,dev-bigdata03.aiads-host.com:9092");
        properties.setProperty("group.id", "test4444");


        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("spider_maoyan_box_dashboard",
                new SimpleStringSchema(), properties);


        //kafka source
        DataStream<String> keyedStream = env.addSource(myConsumer);


        keyedStream.map(new MapFunction<String, Tuple5<String, String, String, String, String>>() {
            @Override
            public Tuple5<String, String, String, String, String> map(String value) throws Exception {


                JSONObject jsonObject = JSON.parseObject(value);
                String splitTotalBox = jsonObject.getString("splitTotalBox");
                String totalBoxInfo = jsonObject.getString("totalBoxInfo");
                String totalBox = jsonObject.getString("totalBox");
                String queryDate = jsonObject.getString("queryDate");
                String splitTotalBoxInfo = jsonObject.getString("splitTotalBoxInfo");
                return Tuple5.of(splitTotalBox, totalBoxInfo, totalBox, queryDate, splitTotalBoxInfo);
            }
        }).addSink(new MongoDBsink());

        env.execute("Flink Streaming Java API From Kafka To Mongodb");
    }
}
