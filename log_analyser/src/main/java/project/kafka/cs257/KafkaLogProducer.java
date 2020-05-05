package project.kafka.cs257;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.BufferedReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class KafkaLogProducer {
	public static void main(String[] commandLineArgs) throws InterruptedException, IOException {
		// validate command line arguments
		if (commandLineArgs.length < 3) {
			System.err.println("usage: KafkaLogProducer <file-input> <Kafka-topic-for-output> <wait-time>");
			System.exit(1);
		}

		// Extract command line input
		String file_input = commandLineArgs[0];
		String output_topic = commandLineArgs[1];
		long wait_time = Long.parseLong(commandLineArgs[2]);

		/*Here, we prepare Properties
		Properties manage the key-value pairs of configuration. In each pair, both are String values
		*/
		Properties properties = new Properties();
		/*
		We know that key is Integer Here
		Values are in the form of String, in this case - logs
		*/
		properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		//We used port as 9092
		properties.put("bootstrap.servers", "localhost:9092");

		KafkaProducer<Integer, String> kafka_producer = new KafkaProducer<Integer, String>(properties);

		BufferedReader buffer_reader = null;
		int log = 0;
		try {
			String item;
			buffer_reader = new BufferedReader(new FileReader(new File(file_input)));
			while ((item = buffer_reader.readLine()) != null) {
				ProducerRecord<Integer, String> producer_record = new ProducerRecord<Integer, String>(output_topic, ++log,
						item);
				/*
				This producer send a string containing a series of key and value pairs with key as a integer
				value as a log entry from the log database file that we used
				*/

				kafka_producer.send(producer_record);
				System.out.println("Log record is sent to the topic " + log + ":" + item);
				// Wait time is added to throttle the IO.
				Thread.sleep(wait_time);
			}
		} catch (IOException io_exception) {
			io_exception.printStackTrace();
		}
		//We need to close buffer reader as well as kafka producer client
		buffer_reader.close();
		kafka_producer.close();
	}
}
