package com.jia.flink.api.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * ClassName: KafkaSourceTest
 * Package: com.jia.flink.api.source
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 13:52
 * @Version 1.0
 */
public class KafkaSourceTest {


	public static void main(String[] args) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
				.setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
				.setGroupId("flink")
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.setTopics("topic_b")
				.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
				.build();
		DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
		ds.print();

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}


}
