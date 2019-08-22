package morgan.mu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Title: SocketWindowWordCountJava
 * @Description: 通过socket模拟产生数据  每隔1秒最近两秒的数据进行汇总
 * @Author: YuSong.Mu
 * @Date: 2019/5/6 16:17
 */
public class SocketWindowWordCountJava {

    public static void main(String[] args) throws Exception {

        //获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hostname = "localhost";
        int port = 9000;
        String delimiter = "\n";

        DataStreamSource<String> stream = env.socketTextStream(hostname, port, delimiter);

        SingleOutputStreamOperator<WordCount> windowCount = stream.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> out) throws Exception {
                String[] split = value.split(" ");
                for (String word : split) {
                    out.collect(new WordCount(word, 1L));
                }
            }
        }).keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum("value");

        windowCount.print().setParallelism(1);

        //必须调用execute算法触发执行
        env.execute("Socket Window Word Count");

    }

    public static class WordCount {
        public String word;
        public Long value;

        public WordCount() {
        }

        public WordCount(String word, Long value) {
            this.value = value;
            this.word = word;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", value=" + value +
                    '}';
        }
    }
}
