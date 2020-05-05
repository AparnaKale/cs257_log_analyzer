package project.kafka.cs257;

import java.util.*;
import java.io.IOException;
import org.apache.kafka.clients.consumer.*;



public class KafkaLogConsumer {
	static HashMap<String,String> allTimeZoneResponseCodeCountHM = new HashMap<String,String>();
	static HashMap<String,String> topTimeZone503HM = new LinkedHashMap<String,String>(); 

	public static void main(String[] args) throws IOException {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("auto.offset.reset", "earliest");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		ArrayList<String> topics = new ArrayList<String>();
		topics.add(args[0]);
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
		kafkaConsumer.subscribe(topics);
		long duration = 1000;
		int count = 0, waitCount = 0;
		System.out.println("");
		while (true) {
			ConsumerRecords<String, String> fetchedRecords = kafkaConsumer.poll(duration);
			Iterator<ConsumerRecord<String, String>> iter = fetchedRecords.iterator();
			count = 0;
			while (iter.hasNext()) {
				count++;
				ConsumerRecord<String, String> record = iter.next();
				String recordKey=record.key();
				String recordVal=record.value();
				if(!recordVal.equals("[]")) {
					recordVal=recordVal.replaceAll("\\[|\\(", "").replace(")]", "), ");
					String[] keyValuePairs=recordVal.split("\\), ");
					if(recordKey.equals("TimeZoneResponseCodeCounts")) {
						System.out.println("Read analyisis - Time Zone + Response Code Count : "+record.value());
						for (int i=0;i<keyValuePairs.length;i++) {
							String[] keyValue = keyValuePairs[i].split(",");		
							allTimeZoneResponseCodeCountHM.put(keyValue[0], keyValue[1]);
						}
					}else if(recordKey.equals("TopTimeZone503")) {
						System.out.println("Read analyisis - Top Time Zone With 503 Response Code : "+record.value());
						for (int i=keyValuePairs.length-1;i>=0;i--) {
							String[] keyValue = keyValuePairs[i].split(",");
							String[] keyParts = keyValue[0].split("\\+"); 
							topTimeZone503HM.remove(keyParts[0]);
							topTimeZone503HM.put(keyParts[0], keyValue[1]);
							
						}
					}
				}
			}	
			if (count == 0) waitCount++;
			else {
				System.out.println("Updated analysis - Time Zone + Response Code Count : "+allTimeZoneResponseCodeCountHM.toString());
				List<String> keys = new ArrayList<>(topTimeZone503HM.keySet());
				HashMap<String,String> topTimeZone503HMTemp = new LinkedHashMap<String,String>(); 
				for(int i = keys.size()-1; i>=0;i--) {
					topTimeZone503HMTemp.put(keys.get(i), topTimeZone503HM.get(keys.get(i)));
				}
				System.out.println("Updated analysis - Top Time Zone With 503 Response Code  : "+topTimeZone503HMTemp.toString());
				System.out.println("");
				waitCount = 0;
			}
			if (waitCount > 20)	break;
		}
		kafkaConsumer.close();
	}
}
