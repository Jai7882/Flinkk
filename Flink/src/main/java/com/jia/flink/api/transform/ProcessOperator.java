package com.jia.flink.api.transform;

import com.alibaba.fastjson.JSON;
import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ClassName: ProcessOperator
 * Package: com.jia.flink.api.transform
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 19:13
 * @Version 1.0
 */
public class ProcessOperator {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Event> ds = env.addSource(new ClickSource());
		ds.process(new ProcessFunction<Event, String>() {
			@Override
			public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
				out.collect(JSON.toJSONString(value));
			}
		}).print();

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
