package com.jia.flink.api.timeandwindow;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * ClassName: WindowAssignersTest
 * Package: com.jia.flink.api.timeandwindow
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/10 16:47
 * @Version 1.0
 */
public class WindowAssignersTest {



	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(
				WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
						.withTimestampAssigner((event, ts) -> event.getTs())
		);
		ds.print();

//		ds.map(event-> Tuple1.of(1L))
//				.returns(Types.TUPLE(Types.LONG))
//				.countWindowAll(10,5)
//				.sum(0)
//				.print("非按键分区窗口计数");

//		ds.map(event-> Tuple1.of(1L))
//				.returns(Types.TUPLE(Types.LONG))
//				.windowAll(
//						TumblingEventTimeWindows.of(Time.seconds(10))
//				)
//				.sum(0)
//				.print("滚动事件窗口计数");

//		ds.map(event-> Tuple1.of(1L))
//				.returns(Types.TUPLE(Types.LONG))
//				.windowAll(
//						TumblingProcessingTimeWindows.of(Time.seconds(10))
//				)
//				.sum(0)
//				.print("滚动处理窗口计数");

//		ds.map(event-> Tuple2.of(event.getUser(),1L))
//				.returns(Types.TUPLE(Types.STRING,Types.LONG))
//				.keyBy(e->e.f0)
//				.countWindow(
//						5,3 //滚动或滑动
//				)
//				.sum(1)
//				.print("按键分区滚动处理窗口计数");

		ds.map(event-> Tuple2.of(event.getUser(),1L))
				.returns(Types.TUPLE(Types.STRING,Types.LONG))
				.keyBy(e->e.f0)
				.window(
						//滚动窗口
						TumblingProcessingTimeWindows.of(Time.seconds(8)) //处理事件滚动窗口 8 s处理时间
//						TumblingEventTimeWindows.of(Time.seconds(8)) //事件时间滚动窗口8s

						//滑动窗口
//						SlidingProcessingTimeWindows.of(Time.seconds(8L),Time.seconds(5)) //处理时间8s 滑动时间5s 滑动窗口
//						SlidingEventTimeWindows.of(Time.seconds(8),Time.seconds(5)) //事件时间8秒大小 5秒滑动

						//会话窗口
//						EventTimeSessionWindows.withGap(Time.seconds(8)) //事件时间超时8秒的会话窗口
//						ProcessingTimeSessionWindows.withGap(Time.seconds(8)) //处理时间超时8秒的会话窗口

						//全局窗口
//						GlobalWindows.create()
				)
				.sum(1)
				.print();
//				.trigger() //全局窗口需要定义的触发器

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
