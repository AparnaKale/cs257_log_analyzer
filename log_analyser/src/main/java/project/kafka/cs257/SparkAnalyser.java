package project.kafka.cs257;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.*;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.kafka09.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.regex.Pattern;
import scala.Serializable;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.StateSpec;
import scala.Tuple2;

public class SparkAnalyser {
	public static void main(String[] args) {
		if (args.length != 4) {
			System.err.println("Input should be: <input-topic> <output-topic> <cg> <interval>");
			System.exit(1);
		}

		String inputTopic = args[0];
		Pattern inputTopicPattern = Pattern.compile(inputTopic, Pattern.CASE_INSENSITIVE);
		String outputTopic = args[1];
		int interval = Integer.parseInt(args[3]);

		Map<String, Object> consumerParameters = new HashMap<String, Object>();
		consumerParameters.put("bootstrap.servers", "localhost:9092");
		consumerParameters.put("auto.offset.reset", "earliest");
		consumerParameters.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		consumerParameters.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		Properties producerProperties = new Properties();
		producerProperties.put("bootstrap.servers", "localhost:9092");
		producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		JavaStreamingContext ssc = new JavaStreamingContext("local[2]", "SparkAnalyser", new Duration(interval));
		ssc.sparkContext().getConf().set("spark.streaming.stopGracefullyOnShutdown", "true");
		ssc.checkpoint("./checkpoint");

		JavaInputDStream<ConsumerRecord<Integer, String>> unprocessed_LogInputDStream = KafkaUtils.createDirectStream(
				ssc, LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<Integer, String>SubscribePattern(inputTopicPattern, consumerParameters));

		// generate processedLog_DStream
		JavaDStream<LogData> processedLog_DStream = unprocessed_LogInputDStream.map(LogData.parseUnprocessedLog)
				.filter(LogData.checkprocessedLog).cache();

		JavaPairDStream<String, Integer> timeZoneResponseCodeCountPairs = processedLog_DStream
				.mapToPair(LogData.mapTZWithRC).reduceByKey(LogData.reduceBySumTZ);

		JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> timeZoneResponseCodeCountDstream = timeZoneResponseCodeCountPairs
				.mapWithState(StateSpec.function(mapKeyWithState_String).initialState(getInitialRDD_StringKey(ssc)));

		timeZoneResponseCodeCountDstream.foreachRDD(rdd -> {
			if (rdd.count() > 0) {
				KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(producerProperties);
				ProducerRecord<String, String> prodRecord = new ProducerRecord<String, String>(outputTopic,
						"TimeZoneResponseCodeCounts", rdd.collect().toString());
				kafkaProducer.send(prodRecord);
				kafkaProducer.close();
			}
		});

		// calculate top 5 time zones with 503 response code
		JavaPairDStream<String, Integer> timeZoneResponseCode503CountPairs = timeZoneResponseCodeCountPairs
				.filter(LogData.checkRC503);

		JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> timeZoneResponseCode503CountDstream = timeZoneResponseCode503CountPairs
				.mapWithState(StateSpec.function(mapKeyWithState_String).initialState(getInitialRDD_StringKey(ssc)));
		timeZoneResponseCode503CountDstream.foreachRDD(rdd -> {
			if (rdd.count() > 0) {
				List<Tuple2<String, Integer>> topTimeZoneResponseCode503 = rdd.takeOrdered(5,
						(Comparator<Tuple2<String, Integer>> & Serializable) (o1, o2) -> {
							return o2._2().compareTo(o1._2());
						});
				KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(producerProperties);
				ProducerRecord<String, String> prodRecord = new ProducerRecord<String, String>(outputTopic,
						"TopTimeZone503", topTimeZoneResponseCode503.toString());
				kafkaProducer.send(prodRecord);
				kafkaProducer.close();
			}
		});

		ssc.start();

		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			System.out.println("Spark Analyser has been interrupted.");
		}
	}

	static Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mapKeyWithState_String = (
			stringKey, one, eachState) -> {
		int sum = one.orElse(0) + (eachState.exists() ? eachState.get() : 0);
		eachState.update(sum);
		return new Tuple2<>(stringKey, sum);
	};

	static JavaPairRDD<String, Integer> getInitialRDD_StringKey(JavaStreamingContext ssc) {
		return ssc.sparkContext().parallelizePairs(new ArrayList<Tuple2<String, Integer>>());
	}
}
