package project.kafka.cs257;

import scala.Tuple2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.lang.String;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.Serializable;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class LogData implements Serializable {
	static final long serialVersionUID = 1L;
	String uID;
	String ipAdd;
	String clientID;
	String method;
	String dtString;
	String timeZone;
	int rCode;
	String res;
	String protocol;
	long sizeOfContent;
	boolean isLogValid;

	LogData() {
		this.isLogValid = false;
	}

	LogData(String ipAdd, String clientID, String uID, String dt, String method, String res, String protocol, String rCode, String sizeOfContent) {
		this.uID = uID;
		this.ipAdd = ipAdd;
		this.clientID = clientID;
		this.method = method;
		this.res = res;
		this.dtString = dt;
		this.timeZone = dt.substring(dt.length() - 5);
		this.rCode = Integer.parseInt(rCode);
		this.protocol = protocol;
		this.sizeOfContent = Long.parseLong(sizeOfContent);
		this.isLogValid = true;
	}

	static Function<ConsumerRecord<Integer, String>, LogData> parseUnprocessedLog = (rec) -> {
		final String REGEX = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
		Pattern ACCESS_LOG_PATTERN = Pattern.compile(REGEX);
		Matcher match = ACCESS_LOG_PATTERN.matcher(rec.value());
		if (!match.find()) {
			return new LogData();
		} else {
			return new LogData(match.group(1), match.group(2), match.group(3), match.group(4), match.group(5),
					match.group(6), match.group(7), match.group(8), match.group(9));
		}
	};

	static Function2<Integer, Integer, Integer> reduceBySumTZ = (v1, v2) -> {
		return v1 + v2;
	};

	static PairFunction<LogData, String, Integer> mapTZWithRC = (log) -> {
		String key = log.getTimeZone() + "+" + log.getRC();
		return new Tuple2<String, Integer>(key, 1);
	};

	static Function<Tuple2<String, Integer>, Boolean> checkRC503 = (log) -> {
		String[] temp = log._1().split("\\+");
		if (temp[1].equals("503"))
			return true;
		return false;
	};

	static Function<LogData, Boolean> checkprocessedLog = (log) -> {
		return log.isValid();
	};

	String getTimeZone() {
		return timeZone;
	}

	int getRC() {
		return rCode;
	}

	boolean isValid() {
		return isLogValid;
	}
}