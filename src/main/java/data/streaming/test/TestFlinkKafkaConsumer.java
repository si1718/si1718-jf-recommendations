package data.streaming.test;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import data.streaming.auxiliar.FileSinkFunction;
import data.streaming.auxiliar.FlatMapFunctionImpl;
import data.streaming.dto.KeywordDTO;
import data.streaming.dto.TweetDTO;
import data.streaming.utils.LoggingFactory;
import data.streaming.utils.Utils;

public class TestFlinkKafkaConsumer {

	private static final String PATH = "./out/data.csv";

	public static void main(String... args) throws Exception {

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = LoggingFactory.getCloudKarafkaCredentials();

		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<>(
				props.getProperty("CLOUDKARAFKA_TOPIC").trim(), new SimpleStringSchema(), props));
		SinkFunction<KeywordDTO> sinkFunction = new FileSinkFunction(PATH);
		FlatMapFunction<TweetDTO, KeywordDTO> function = new FlatMapFunctionImpl();
		stream.filter(x -> Utils.isValid(x)).map(x -> Utils.createTweetDTO(x))
				.flatMap(function ).addSink(sinkFunction);

		// execute program
		env.execute("Twitter Streaming Consumer");
	}

}
