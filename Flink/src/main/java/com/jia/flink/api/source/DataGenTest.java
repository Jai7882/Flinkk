package com.jia.flink.api.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.UUID;

/**
 * ClassName: DataGenTest
 * Package: com.jia.flink.api.source
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 14:35
 * @Version 1.0
 */
public class DataGenTest {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataGeneratorSource<String> generatorSource = new DataGeneratorSource<>(in -> UUID.randomUUID().toString() +"-->" +in, 1000L,
				RateLimiterStrategy.perSecond(1),Types.STRING);
		DataStreamSource<String> ds = env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "dataGen");
		ds.print();

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}
}
