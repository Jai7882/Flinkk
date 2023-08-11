package com.jia.flink.api.timeandwindow;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;

/**
 * ClassName: PvUv
 * Package: com.jia.flink.api.timeandwindow
 * Description:
 * <p>
 * <p>
 * 全窗口函数(全量聚合)
 */
public class ProcessWindowFunctionTest {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(
				WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
						.withTimestampAssigner((event, ts) -> event.getTs())
		);
		ds.print();
		// 统计10秒内每名用户的点击次数 并包含窗口信息
		ds.map(e -> Tuple2.of(e.getUser(), 1L))
				.returns(Types.TUPLE(Types.STRING, Types.LONG))
				.keyBy(e -> e.f0)
				.window(
						TumblingEventTimeWindows.of(Time.seconds(10))
				).process(

						new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
							/**
							 *
							 * @param key The key for which this window is evaluated.
							 * @param context The context in which the window is being evaluated.
							 * @param elements The elements in the window being evaluated.
							 * @param out A collector for emitting elements.
							 * @throws Exception
							 */
							@Override
							public void process(String key, ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>.Context context,
												Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
								long count = 0L;
								for (Tuple2<String, Long> element : elements) {
									count ++;
								}
								out.collect("窗口[" + context.window().getStart() + " ~ " + context.window().getEnd() + ") , 用户" + key
								+ " 访问次数为: " + count );
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
