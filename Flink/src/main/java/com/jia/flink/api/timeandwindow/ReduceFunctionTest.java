package com.jia.flink.api.timeandwindow;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * ClassName: ReduceFunctionTest
 * Package: com.jia.flink.api.timeandwindow
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/10 18:03
 * @Version 1.0
 */
public class ReduceFunctionTest {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(
				WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
						.withTimestampAssigner((event, ts) -> event.getTs())
		);
		ds.print();

		ds.map(e-> Tuple2.of(e.getUser(),1L))
				.returns(Types.TUPLE(Types.STRING,Types.LONG))
				.keyBy(i->i.f0)
				.window(
						TumblingEventTimeWindows.of(Time.seconds(10))
				)
				.reduce(
						(t1,t2)-> Tuple2.of(t1.f0,t1.f1+t2.f1)
				)
				.print();

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
