package utils;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

public class ProducerKafka {
    private final static String[] topic = {"temperature","pression","humidity","city_attributes","weather"};

    public static void produce(String[] pathList) throws IOException {
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        /*
        props.put("message.max.bytes", "41943040");
        props.put("max.request.size", "41943040");
        props.put("replica.fetch.max.bytes", "41943040");
        props.put("fetch.message.max.bytes", "41943040");*/

        //Set acknowledgements for ProducerKafka requests.
        props.put("acks", "all");

        //If the request fails, the ProducerKafka can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the ProducerKafka for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        ArrayList<String> fileContent = new ArrayList<String>();

        for(int x=0;x<topic.length;x++){
            File file=new File(System.getProperty("user.dir")+"/"+pathList[x]);
            String str = FileUtils.readFileToString(file);
            System.out.println(str);
            producer.send(new ProducerRecord<String, String>(topic[x],str));
        }


        producer.close();

    }


}
