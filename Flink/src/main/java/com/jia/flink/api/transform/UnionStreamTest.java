package com.jia.flink.api.transform;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: UnionStreamTest
 * Package: com.jia.flink.api.transform
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 19:45
 * @Version 1.0
 */
public class UnionStreamTest {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<String> ds1 = env.fromElements("a", "b", "c", "d", "a", "c");
		DataStreamSource<String> ds2 = env.fromElements("c", "b", "c", "d", "g", "c");
		DataStreamSource<String> ds3 = env.fromElements("n", "b", "d", "d", "a", "j");
		DataStream<String> union = ds1.union(ds2, ds3);
		union.print("union");

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
