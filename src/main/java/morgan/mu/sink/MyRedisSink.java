package morgan.mu.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author yusong.Mu
 * @version 1.0
 * @Description: flink将数据写入redis
 * @date 2019/10/2 17:06
 */
public class MyRedisSink implements RedisMapper<Tuple4<String,Integer,String, String>> {



    @Override
    public RedisCommandDescription getCommandDescription() {

        RedisCommandDescription channel_code =
                new RedisCommandDescription(RedisCommand.HSET, "channel_code");
        return channel_code;
    }

    @Override
    public String getKeyFromData(Tuple4<String, Integer, String, String> stringIntegerStringStringTuple4) {
        return stringIntegerStringStringTuple4.f0;
    }

    @Override
    public String getValueFromData(Tuple4<String, Integer, String, String> stringIntegerStringStringTuple4) {
        return stringIntegerStringStringTuple4.f1+"::"+stringIntegerStringStringTuple4.f2+"::"+stringIntegerStringStringTuple4.f3;
    }


}
