package com.jia.flink.api.sink;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: UserDefineSink
 * Package: com.jia.flink.api.sink
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/9 10:13
 * @Version 1.0
 */
public class UserDefineSink {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Event> ds = env.addSource(new ClickSource());

		ds.addSink(new MySinkFunction());

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
