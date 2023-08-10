package com.jia.flink.api.timeandwindow;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * ClassName: AggregateFunction
 * Package: com.jia.flink.api.timeandwindow
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/10 18:11
 * @Version 1.0
 */
// 聚合函数
public class AggregateFunctionTest {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(
				WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
						.withTimestampAssigner((event, ts) -> event.getTs())
		);
		ds.print();
		// 统计十秒内的UV (独立访客数)
//		ds.windowAll(
//				TumblingEventTimeWindows.of(Time.seconds(10))
//		).aggregate(
//				/**
//				 * 通过hashset中放字符串维护数据中每个user
//				 * 因为set的特性自动去重
//				 */
//				new AggregateFunction<Event, HashSet<String>, Integer>() {
//					//创建累加器 只执行一次
//					@Override
//					public HashSet<String> createAccumulator() {
//						return new HashSet<>();
//					}
//
//					//累加过程 每条数据执行一次
//					@Override
//					public HashSet<String> add(Event value, HashSet<String> accumulator) {
//						accumulator.add(value.getUser());
//						return accumulator;
//					}
//
//					//最终获取累加器的结果
//					@Override
//					public Integer getResult(HashSet<String> accumulator) {
//						return accumulator.size();
//					}
//
//					//合并累加器一般不使用
//					@Override
//					public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
//						return null;
//					}
//				}
//		).print();

		ds.keyBy(e -> true)
				.window(
						TumblingEventTimeWindows.of(Time.seconds(10))
				).aggregate(
						new AggregateFunction<Event, HashSet<String>, Integer>() {
							//创建累加器 只执行一次
							@Override
							public HashSet<String> createAccumulator() {
								return new HashSet<>();
							}

							//累加过程 每条数据执行一次
							@Override
							public HashSet<String> add(Event value, HashSet<String> accumulator) {
								accumulator.add(value.getUser());
								return accumulator;
							}

							//最终获取累加器的结果
							@Override
							public Integer getResult(HashSet<String> accumulator) {
								return accumulator.size();
							}

							//合并累加器一般不使用
							@Override
							public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
								return null;
							}
						}
				).print();

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


}
