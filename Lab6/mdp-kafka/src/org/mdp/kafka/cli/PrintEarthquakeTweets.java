package org.mdp.kafka.cli;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mdp.kafka.def.KafkaConstants;

public class PrintEarthquakeTweets {
	public static final String[] EARTHQUAKE_SUBSTRINGS = new String[] { "terremoto", "temblor", "sismo", "quake" };
	public static final int FIFO_SIZE = 50; // detect this number in a window
	public static final int WARNING_WINDOW_SIZE = 50000; // create warning for this window
	public static final int CRITICAL_WINDOW_SIZE = 25000; // create critical message for this window
	
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
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
						// if so print it out to the console
						if(lowercase.contains(ek)){
							System.out.println(record.value());
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
