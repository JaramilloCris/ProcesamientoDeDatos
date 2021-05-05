package org.mdp.kafka.cli;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mdp.kafka.def.KafkaConstants;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Properties;
import java.util.UUID;

public class BurstDetector {
    public static final String[] EARTHQUAKE_SUBSTRINGS = new String[] { "terremoto", "temblor", "sismo", "quake" };
    public static final int FIFO_SIZE = 50; // detect this number in a window
    public static final int WARNING_WINDOW_SIZE = 50000; // create warning for this window
    public static final int CRITICAL_WINDOW_SIZE = 25000; // create critical message for this window


    public static void main(String[] args) throws FileNotFoundException, IOException {
        if(args.length==0 || args.length>2 || (args.length==2 && !args[1].equals("replay"))){
            System.err.println("Usage [inputTopic] [replay]");
            return;
        }

        Properties props = KafkaConstants.PROPS;
        if(args.length==2){ // if we should replay stream from the start
            // randomise consumer ID for kafka doesn't track where it is
            props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
            // tell kafka to replay stream for new consumers
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(args[0]));

        final int X = 50;
        final int MAYOR_Y = 25;
        final int MINOR_Y = 50;
        LinkedList<ConsumerRecord<String, String>> burst = new LinkedList<>();

        int c = 0;
        boolean mayor_burst = false;
        boolean minor_burst = false;
        try{
            while (true) {
                // every ten milliseconds get all records in a batch
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));

                // for all records in the batch
                for (ConsumerRecord<String, String> record : records) {
                    burst.add(record);
                    c+=1;
                    if (c == X) {
                        long k_inicial = burst.getFirst().timestamp()/1000;
                        long k_ultimo = burst.getLast().timestamp()/1000;
                        long dif = k_ultimo - k_inicial;
                        // Burst Mayor -> Burst menor
                        if (dif <= MAYOR_Y) {
                            mayor_burst = true;
                            minor_burst = true;
                            System.out.println("MAYOR_BURST");
                            System.out.println();
                        // Burst Menor
                        } else if (dif <= MINOR_Y && dif > MAYOR_Y) {
                            minor_burst = true;
                            System.out.println();
                        }
                    } else if (c > X) {

                    }
                }
                System.out.println();
            }
        } finally{
            consumer.close();
        }
    }
}
