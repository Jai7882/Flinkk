package com.jia.flink.api.sink;

import com.alibaba.fastjson.JSON;
import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.jline.utils.Log;

import java.time.Duration;

/**
 * ClassName: FileSinkTest
 * Package: com.jia.flink.api.sink
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/8 16:17
 * @Version 1.0
 */
public class FileSinkTest {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		env.enableCheckpointing(2000);
		SingleOutputStreamOperator<String> ds = env.addSource(new ClickSource()).map(JSON::toJSONString);
		FileSink<String> fileSink = FileSink.<String>forRowFormat(new Path("output"), new SimpleStringEncoder<String>()).withRollingPolicy(DefaultRollingPolicy.builder()
						.withMaxPartSize(MemorySize.parse("10m"))
						.withInactivityInterval(Duration.ofSeconds(10))
						.withRolloverInterval(Duration.ofSeconds(5))
						.build())
				.withBucketCheckInterval(1000L)
				.withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH-mm"))
				.withOutputFileConfig(OutputFileConfig.builder()
						.withPartPrefix("flinkSource")
						.withPartSuffix(".txt")
						.build()).build();
		ds.sinkTo(fileSink);

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
