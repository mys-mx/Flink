package morgan.mu.flinkFromKafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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


        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("192.168.137.14", 9200, "http"));
        ElasticsearchSink.Builder<Tuple6<String, String, Integer, String, String, String>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple6<String, String, Integer, String, String, String>>() {
                    public IndexRequest createIndexRequest(Tuple6<String, String, Integer, String, String, String> element) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", element.f0);
                        map.put("name", element.f1);
                        map.put("age", element.f2);
                        map.put("school", element.f3);
                        map.put("subject", element.f4);
                        map.put("date", element.f5);
                        System.out.println(map);
                        return Requests.indexRequest()
                                .index("flink")
                                .type("test")
                                .source(map);
                    }

                    public UpdateRequest updateIndexRequest(Tuple6<String, String, Integer, String, String, String> element) throws IOException {
                        UpdateRequest updateRequest = new UpdateRequest();
                        //设置表的index和type,必须设置id才能update
//                        Map map = SqlParse.sqlParse("select count(distinct userId) as uv ,behavior from userTable group by behavior", element);
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", element.f0);
                        map.put("name", element.f1);
                        map.put("age", element.f2);
                        map.put("school", element.f3);
                        map.put("subject", element.f4);
                        map.put("date", element.f5);
                        updateRequest
                                .index("flink")
                                .type("test")
                                //必须设置id
                                .id(element.f0)
                                .doc(map)
                                .upsert(createIndexRequest(element));
                        return updateRequest;
                    }

                    @Override
                    public void process(Tuple6<String, String, Integer, String, String, String> element, RuntimeContext ctx, RequestIndexer indexer) {
                        try {
                            indexer.add(updateIndexRequest(element));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
        );
        /*     必须设置flush参数     */
        //刷新前缓冲的最大动作量
        esSinkBuilder.setBulkFlushMaxActions(1);
        //刷新前缓冲区的最大数据大小（以MB为单位）
        esSinkBuilder.setBulkFlushMaxSizeMb(500);
        //论缓冲操作的数量或大小如何都要刷新的时间间隔
        esSinkBuilder.setBulkFlushInterval(5000);
        map.addSink(esSinkBuilder.build());


        env.execute("Flink Streaming Java API From Kafka To ElasticSearch ");
    }
}
