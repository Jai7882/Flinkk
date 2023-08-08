package com.jia.flink.api.transform;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import com.jia.flink.pojo.WordCount;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: ReduceAggOperator
 * Package: com.jia.flink.api.transform
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 18:11
 * @Version 1.0
 */
public class ReduceAggOperator {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Event> ds = env.addSource(new ClickSource());

		// 求每个url的点击次数
		ds.map(event -> new WordCount(event.getUrl(),1L)).keyBy(WordCount::getWord).reduce((a,b)->new WordCount(a.getWord(),a.getCount()+b.getCount()))
				.print("reduce");



		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

}
