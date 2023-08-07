package com.jia.flink.api.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: ImplementFunction
 * Package: com.jia.flink.api.transform
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 15:51
 * @Version 1.0
 */
public class ImplementFunction  {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);

		SingleOutputStreamOperator<Tuple2<String,Long>> flatMap = ds.flatMap((s, c) -> {
			String[] words = s.split(" ");
			for (String word : words) {
				c.collect(Tuple2.of(word, 1L));
			}
		});
		flatMap.returns(Types.TUPLE(Types.STRING,Types.LONG)).keyBy(v->v.f0).sum(1).print("sum");


		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}
}
