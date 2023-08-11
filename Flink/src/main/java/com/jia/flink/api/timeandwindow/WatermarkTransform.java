package com.jia.flink.api.timeandwindow;

import com.alibaba.fastjson.JSON;
import com.jia.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * ClassName: WatermarkTransform
 * Package: com.jia.flink.api.timeandwindow
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/9 17:14
 * @Version 1.0
 */
public class WatermarkTransform {

	public static void main(String[] args) {

		Configuration conf = new Configuration();
		conf.setInteger("rest.port",5678);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
		env.setParallelism(1);
		SingleOutputStreamOperator<Event> ds1 = env.socketTextStream("hadoop102", 8888)
				.map(line -> {
					String[] words = line.split(",");
					return new Event(words[0].trim(), words[1].trim(), Long.valueOf(words[2].trim()));
				}).assignTimestampsAndWatermarks(
						WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
								.withTimestampAssigner((event, ts) -> event.getTs())
				).name("8888");

		SingleOutputStreamOperator<Event> ds2 = env.socketTextStream("hadoop102", 9999)
				.map(line -> {
					String[] words = line.split(",");
					return new Event(words[0].trim(), words[1].trim(), Long.valueOf(words[2].trim()));
				}).assignTimestampsAndWatermarks(
						WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
								.withTimestampAssigner((event, ts) -> event.getTs())
				).name("9999");

		ds1.union(ds2).map(JSON::toJSONString).print();

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
