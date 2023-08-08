package com.jia.flink.api.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ClassName: ConnectStreamTest
 * Package: com.jia.flink.api.transform
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 19:48
 * @Version 1.0
 */
public class ConnectStreamTest {
	public static void main(String[] args) {
		//将两条流连接合并到一起 两条流中的数据类型可以不同
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<String> ds1 = env.fromElements("a", "b", "c", "d", "a", "c");
		DataStreamSource<Integer> ds2 = env.fromElements(1, 2, 3, 4, 5, 1);
		ConnectedStreams<String, Integer> connect = ds1.connect(ds2);
		SingleOutputStreamOperator<String> ds = connect.process(new CoProcessFunction<String, Integer, String>() {
			@Override
			public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
				out.collect(value.toUpperCase());
			}

			@Override
			public void processElement2(Integer value, Context ctx, Collector<String> out) throws Exception {
				out.collect(value.toString());
			}
		});
		ds.print("connect");
		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
