package morgan.mu.kafkaProducer;


import org.apache.kafka.clients.producer.*;

import java.util.Properties;


public class JavaKafkaProducer {


    public static void main(String[] args) {

        String topic = "test";

        Properties props = new Properties();
        props.put("bootstrap.servers", "hdp-dn-01:9092,hdp-dn-02:9092,hdp-dn-03:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", 1);

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        while (true) {

            producer.send(new ProducerRecord<String, String>(topic, "Hi"));

            producer.send(new ProducerRecord<String, String>(topic, "Morgan"), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        System.out.println(metadata.toString());//org.apache.kafka.clients.producer.RecordMetadata@1d89e2b5
                        System.out.println(metadata.offset());//1
                    }
                }
            });
            producer.flush();
//            producer.close();
        }

    }

}
