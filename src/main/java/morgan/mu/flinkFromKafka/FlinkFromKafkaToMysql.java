package morgan.mu.flinkFromKafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import morgan.mu.sink.MysqlSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * @author yusong.Mu
 * @version 1.0
 * @Description: flink消费kafka数据到mysql
 * @date 2019/8/24 15:56
 */
public class FlinkFromKafkaToMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(500);


        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "hdp-dn-01:9092,hdp-dn-02:9092,hdp-dn-03:9092");
        properties.setProperty("group.id", "test11");

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("test123",
                new SimpleStringSchema(), properties);

        DataStreamSource<String> stringDataStreamSource = env.addSource(myConsumer);

        stringDataStreamSource.map(new MapFunction<String, Tuple4<String, Integer, String, String>>() {
            @Override
            public Tuple4<String, Integer, String, String> map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                return Tuple4.of(jsonObject.getString("name"),
                        jsonObject.getIntValue("age"),
                        jsonObject.getString("school"),
                        jsonObject.getString("subject"));
            }
        }).addSink(new MysqlSink());

        env.execute("flink streaming from kafka to mysql");

    }
}
