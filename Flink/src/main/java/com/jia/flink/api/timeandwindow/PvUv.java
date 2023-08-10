package com.jia.flink.api.timeandwindow;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * ClassName: PvUv
 * Package: com.jia.flink.api.timeandwindow
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/10 18:34
 * @Version 1.0
 */
public class PvUv {
	// PV 页面浏览量
	// UV 独立访客数
	// PV/UV 的比值表示人均重复访问量 代表了用户黏度
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(
				WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
						.withTimestampAssigner((event, ts) -> event.getTs())
		);
		ds.print();

		ds.keyBy(e -> true)
				.window(
						TumblingEventTimeWindows.of(Time.seconds(10))
				).aggregate(
						new AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double>() {
							@Override
							public Tuple2<Long, HashSet<String>> createAccumulator() {
								return Tuple2.of(0L,new HashSet<>());
							}

							@Override
							public Tuple2<Long, HashSet<String>> add(Event value, Tuple2<Long, HashSet<String>> accumulator) {
								// pv = accumulator.f0+1   uv = accumulator.f1
								accumulator.f1.add(value.getUser());
								long count = accumulator.f0 + 1L;
								return Tuple2.of(count,accumulator.f1);
							}

							@Override
							public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
								return accumulator.f0.doubleValue()/accumulator.f1.size();
							}

							@Override
							public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> a, Tuple2<Long, HashSet<String>> b) {
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
