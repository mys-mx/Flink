package morgan.mu.kafkaProducer;


import org.apache.kafka.clients.producer.*;

import java.util.Properties;


public class JavaKafkaProducer {


    public static void main(String[] args) throws InterruptedException {

        String topic = "flink-es";

        Properties props = new Properties();
        props.put("bootstrap.servers", "hdp-dn-01:9092,hdp-dn-02:9092,hdp-dn-03:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", 1);

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        while (true) {

            producer.send(new ProducerRecord<String, String>(topic, "{\"id\":\"1\",\"name\":\"LiMing\",\"age\":17,\"school\":\"北京大学\",\"subject\":\"bigdata\"}"), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception!=null){
                        exception.printStackTrace();
                    }else {
                        System.out.println(metadata.toString());
                        System.out.println(metadata.offset());
                    }
                }
            });

            Thread.sleep(1000);
            producer.send(new ProducerRecord<String, String>(topic, "{\"id\":\"2\",\"name\":\"XiaoHong\",\"age\":17,\"school\":\"河北北方学院南校区\",\"subject\":\"bigdata\"}"), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception!=null){
                        exception.printStackTrace();
                    }else {
                        System.out.println(metadata.toString());
                        System.out.println(metadata.offset());
                    }
                }
            });
            Thread.sleep(1000);

            producer.send(new ProducerRecord<String, String>(topic, "{\"id\":\"3\",\"name\":\"DaZhuang\",\"age\":17,\"school\":\"清华大学\",\"subject\":\"bigdata\"}"), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception!=null){
                        exception.printStackTrace();
                    }else {
                        System.out.println(metadata.toString());
                        System.out.println(metadata.offset());
                    }
                }
            });
            producer.flush();
//            producer.close();
        }

    }

}
