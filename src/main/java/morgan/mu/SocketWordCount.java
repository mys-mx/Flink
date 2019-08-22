package morgan.mu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * @Title: SocketWordCount
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2019/5/6 17:48
 */
public class SocketWordCount {
    public static void main(String[] args) throws Exception {

        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("no port use default port");
            port = 9000;
        }


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hostname = "";
        String delimiter = " ";

        DataStreamSource<String> stream = env.socketTextStream(hostname, port, delimiter);

        SingleOutputStreamOperator<WordCount> sum = stream.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String word, Collector<WordCount> out) throws Exception {
                String[] split = word.split(" ");
                for (String a : split) {
                    out.collect(new WordCount(a, 1));
                }
            }
        }).keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1)).sum("value");
        sum.print().setParallelism(1);

        env.execute("word count");

    }

    public static class WordCount {
        String word;
        int value;

        public WordCount() {
        }

        public WordCount(String word, int value) {
            this.word = word;
            this.value = value;
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
