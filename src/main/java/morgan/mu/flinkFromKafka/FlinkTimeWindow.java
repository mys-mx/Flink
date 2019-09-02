package morgan.mu.flinkFromKafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author yusong.Mu
 * @version 1.0
 * @Description: flink每小时数据的统计
 * @date 2019/8/31 18:48
 */
public class FlinkTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(500);

        //数据产生的时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hdp-dn-01:9092,hdp-dn-02:9092,hdp-dn-03:9092");
        properties.setProperty("group.id", "flink_kafka");

        FlinkKafkaConsumer010<ObjectNode> objectNodeFlinkKafkaConsumer010 = new FlinkKafkaConsumer010<ObjectNode>("flink_kafka" ,
                new JSONKeyValueDeserializationSchema(false), properties);

        DataStreamSource<ObjectNode> myConsumer = env.addSource(objectNodeFlinkKafkaConsumer010);

        //设置时间窗口
        SingleOutputStreamOperator<ObjectNode> myFlinkWindow = myConsumer.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<ObjectNode>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(ObjectNode jsonNodes) {
//                System.out.println(jsonNodes.get("value").get("time").longValue());
                return jsonNodes.get("value").get("time").longValue();
            }
        });


        KeyedStream<Tuple2<String, Long>, Tuple> tuple2TupleKeyedStream = myFlinkWindow.map(new MapFunction<ObjectNode, Tuple2<String, Long>>() {

//            @Override
            public Tuple2<String, Long> map(ObjectNode jsonNodes) throws Exception {
                return Tuple2.of(jsonNodes.get("value").get("time").longValue()+"",
                        1l);
            }
        }).keyBy(0);

        //每隔1秒算过去3秒的数据
        tuple2TupleKeyedStream.timeWindow(Time.seconds(1),Time.seconds(3)).sum(1).print();
        env.execute("flink stream word count");

    }
}
