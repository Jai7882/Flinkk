package com.jia.flink.process;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ClassName: TimerAndTimerServiceTest
 * Package: com.jia.flink.process
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/11 19:02
 * @Version 1.0
 */
public class TimerAndTimerServiceTest {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Event> ds = env.addSource(new ClickSource());
		ds.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
				.withTimestampAssigner((e,ts)->e.getTs()))
				.keyBy(Event::getUser)
				.process(
						new KeyedProcessFunction<String, Event, Event>() {
							@Override
							public void processElement(Event value, KeyedProcessFunction<String, Event, Event>.Context ctx, Collector<Event> out) throws Exception {
								// 处理时间定时器
//								long current = ctx.timerService().currentProcessingTime();
//								ctx.timerService().registerProcessingTimeTimer(current+1000L);

								// 事件时间定时器
								long watermark = ctx.timerService().currentWatermark();
								ctx.timerService().registerEventTimeTimer(watermark+1000L);
								out.collect(value);
							}

							@Override
							public void onTimer(long timestamp, KeyedProcessFunction<String, Event, Event>.OnTimerContext ctx, Collector<Event> out) throws Exception {
								System.out.println("触发定时器");
							}
						}
				)
				.print();


		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
