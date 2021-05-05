package org.mdp.kafka.cli;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mdp.kafka.def.KafkaConstants;

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Properties;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

public class EarthquakeFilter {
    public static final String[] EARTHQUAKE_SUBSTRINGS = new String[] { "terremoto", "temblor", "sismo", "quake" };
    public static int TWEET_ID = 2;
    public static final int FIFO_SIZE = 50; // detect this number in a window
    public static final int WARNING_WINDOW_SIZE = 50000; // create warning for this window
    public static final int CRITICAL_WINDOW_SIZE = 25000; // create critical message for this window


    public static void main(String[] args) throws FileNotFoundException, IOException{
        if(args.length==0 || args.length>3 || (args.length==3 && !args[2].equals("replay"))){
            System.err.println("Usage [inputTopic] [outputTopic] [replay]");
            return;
        }

        Properties props = KafkaConstants.PROPS;
        if(args.length==3){ // if we should replay stream from the start
            // randomise consumer ID for kafka doesn't track where it is
            props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
            // tell kafka to replay stream for new consumers
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(args[0]));

        Producer<String, String> producer= new KafkaProducer<String, String>(KafkaConstants.PROPS);
        try{
            while (true) {
                // every ten milliseconds get all records in a batch
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));

                // for all records in the batch
                for (ConsumerRecord<String, String> record : records) {
                    String lowercase = record.value().toLowerCase();

                    // check if record value contains keyword
                    // (could be optimised a lot)
                    for(String ek: EARTHQUAKE_SUBSTRINGS){
                        // 2017-09-19 05:17:00     2017-09-19 04:37:24     909999845930405888      41925129        QUOTE   es      Funcionario de Chiapas roba 90 colchonetas e impide que sean entregadas a damnificados por sismo. https://t.co/l7yLGp5cYM       10
                        if(lowercase.contains(ek)) {
                            String topic = args[1];
                            producer.send(new ProducerRecord<String,String>(topic, 0, record.timestamp(), record.key(), record.value()));
                            //prevents multiple print of the same tweet
                            break;
                        }
                    }
                }
            }
        } finally{
            consumer.close();
        }
    }
}
