package com.jia.flink.api.timeandwindow;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * ClassName: FlinkEmbeddedWatermark
 * Package: com.jia.flink.api.timeandwindow
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/9 16:57
 * @Version 1.0
 */

// Flink提供的水位线生成策略
//    1. 有序流   forMonotonousTimestamps
//    2. 无序流
public class FlinkEmbeddedWatermark {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.getConfig().setAutoWatermarkInterval(500);
		DataStreamSource<Event> ds = env.addSource(new ClickSource());
		//有序流
//		SingleOutputStreamOperator<Event> assigned = ds.assignTimestampsAndWatermarks(WatermarkStrategy
//				.<Event>forMonotonousTimestamps()
//				.withTimestampAssigner((element, ts) -> element.getTs()));
//		ds.print();

		//乱序流
		SingleOutputStreamOperator<Event> assigned = ds.assignTimestampsAndWatermarks(WatermarkStrategy
				.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
				.withTimestampAssigner((element, ts) -> element.getTs()));

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}

