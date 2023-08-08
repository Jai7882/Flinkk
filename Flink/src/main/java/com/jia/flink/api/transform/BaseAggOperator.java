package com.jia.flink.api.transform;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import com.jia.flink.pojo.WordCount;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: BaseAggOperator
 * Package: com.jia.flink.api.transform
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 17:42
 * @Version 1.0
 */
public class BaseAggOperator {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		ClickSource source = new ClickSource();
		DataStreamSource<Event> ds = env.addSource(source);

		// 求每个user的点击数
//		KeyedStream<WordCount, String> keyBy = ds.map(data -> new WordCount(data.getUser(), 1L)).keyBy(WordCount::getWord);
//		SingleOutputStreamOperator<WordCount> sum = keyBy.sum("count");
//		sum.print("sum");

		// 求每个用户最早点击的数据
		ds.keyBy(Event::getUser).minBy("ts").print("min");





		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

}
