package morgan.mu.flinkFromKafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import morgan.mu.sink.ElasticSearchSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;

import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author yusong.Mu
 * @version 1.0
 * @Description: Java类描述
 * @date 2019/8/25 20:36
 */
public class FlinkFromKafkaToES {

    public static void main(String[] args) throws Exception {
        //flink启动命令
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(500);


        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "hdp-dn-01:9092,hdp-dn-02:9092,hdp-dn-03:9092");
        properties.setProperty("group.id", "flink-es");


        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("flink-es",
                new SimpleStringSchema(), properties);


        //kafka source
        DataStream<String> keyedStream = env.addSource(myConsumer);
        SingleOutputStreamOperator<Tuple6<String, String, Integer, String, String, String>> map = keyedStream.map(new MapFunction<String, Tuple6<String, String, Integer, String, String, String>>() {
            @Override
            public Tuple6<String, String, Integer, String, String, String> map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                return Tuple6.of(jsonObject.getString("id"),
                        jsonObject.getString("name"),
                        jsonObject.getInteger("age"),
                        jsonObject.getString("school"),
                        jsonObject.getString("subject"),
                        df.format(new Date()));
            }
        });


        ElasticsearchSink.Builder<Tuple6<String, String, Integer, String, String, String>> esSinkBuilder =
                ElasticSearchSink.getElasticSearchSink();
        map.addSink(esSinkBuilder.build());


        env.execute("Flink Streaming Java API From Kafka To ElasticSearch ");
    }


}
