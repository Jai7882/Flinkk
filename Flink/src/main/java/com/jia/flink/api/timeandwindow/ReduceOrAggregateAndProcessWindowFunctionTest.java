package com.jia.flink.api.timeandwindow;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import com.jia.flink.pojo.UrlViewCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ClassName: PvUv
 * Package: com.jia.flink.api.timeandwindow
 * Description:
 * <p>
 * <p>
 */
public class ReduceOrAggregateAndProcessWindowFunctionTest {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(
				WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
						.withTimestampAssigner((event, ts) -> event.getTs())
		);
		ds.print();
		// 统计 不考虑性能
//		ds.map(e -> Tuple2.of(e.getUrl(), 1L))
//				.returns(Types.TUPLE(Types.STRING, Types.LONG))
//				.keyBy(e -> e.f0)
//				.window(
//						SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))
//				).process(
//						new ProcessWindowFunction<Tuple2<String, Long>, UrlViewCount, String, TimeWindow>() {
//							/**
//							 *
//							 * @param key The key for which this window is evaluated.
//							 * @param context The context in which the window is being evaluated.
//							 * @param elements The elements in the window being evaluated.
//							 * @param out A collector for emitting elements.
//							 * @throws Exception
//							 */
//							@Override
//							public void process(String key, ProcessWindowFunction<Tuple2<String, Long>, UrlViewCount, String, TimeWindow>.Context context,
//												Iterable<Tuple2<String, Long>> elements, Collector<UrlViewCount> out) throws Exception {
//								Long count = 0L;
//								for (Tuple2<String, Long> element : elements) {
//									count ++ ;
//								}
//								long start = context.window().getStart();
//								long end = context.window().getEnd();
//								UrlViewCount urlViewCount = new UrlViewCount(key, start, end, count);
//								out.collect(urlViewCount);
//							}
//						}
//				).print();
		
				ds.map(e -> Tuple2.of(e.getUrl(), 1L))
				.returns(Types.TUPLE(Types.STRING, Types.LONG))
				.keyBy(e -> e.f0)
				.window(
						TumblingEventTimeWindows.of(Time.seconds(10))
				).aggregate(new AggregateFunction<Tuple2<String, Long>, Long, UrlViewCount>() {
							@Override
							public Long createAccumulator() {
								return 0L;
							}

							@Override
							public Long add(Tuple2<String, Long> value, Long accumulator) {
								return accumulator + 1L;
							}

							@Override
							public UrlViewCount getResult(Long accumulator) {
								return new UrlViewCount(null, null , null, accumulator);
							}

							@Override
							public Long merge(Long a, Long b) {
								return null;
							}
						}, new ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>() {
							@Override
							public void process(String key, ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>.Context context,
												Iterable<UrlViewCount> elements, Collector<UrlViewCount> out) throws Exception {
								UrlViewCount urlViewCount = elements.iterator().next();
								urlViewCount.setUrl(key);
								urlViewCount.setStart(context.window().getStart());
								urlViewCount.setEnd(context.window().getEnd());
								out.collect(urlViewCount);
							}
						}).print();

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
