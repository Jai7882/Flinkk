package com.jia.flink.api.transform;

import com.alibaba.fastjson.JSON;
import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: RichFunctionTest
 * Package: com.jia.flink.api.transform
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 18:21
 * @Version 1.0
 */
public class RichFunctionTest {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Event> ds = env.addSource(new ClickSource());
		ds.map(new RichMapFunction<Event, String>() {
			@Override
			public String map(Event event) throws Exception {
				return JSON.toJSONString(event)+"-->redis";
			}
			@Override
			public void open(Configuration parameters) throws Exception {
				System.out.println("创建Jedis");
			}
			@Override
			public void close() throws Exception {
				System.out.println("关闭Jedis");
			}
		}).print();



		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
