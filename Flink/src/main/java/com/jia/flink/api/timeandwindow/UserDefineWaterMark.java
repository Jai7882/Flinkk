package com.jia.flink.api.timeandwindow;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: UserDefineWaterMark
 * Package: com.jia.flink.api.timeandwindow
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/9 15:53
 * @Version 1.0
 */
public class UserDefineWaterMark {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.getConfig().setAutoWatermarkInterval(500);
		DataStreamSource<Event> ds = env.addSource(new ClickSource());
		ds.assignTimestampsAndWatermarks(
				new MyWatermarkStrategy()
		);
		ds.print();

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	//自定义水位线生成策略对象
	public static class MyWatermarkStrategy implements WatermarkStrategy<Event> {
		@Override
		public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
			return new OrderlessWatermark();
		}

		@Override
		public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
			return new TimestampAssigner<Event>() {
				//从数据中提取时间
				@Override
				public long extractTimestamp(Event element, long recordTimestamp) {
					return element.getTs();
				}
			};
		}
	}

	//有序流每套数据生成水位线
	public static class OrderWatermark implements WatermarkGenerator<Event> {

		private Long maxTs = Long.MIN_VALUE;

		//每条数据生成水位线
		@Override
		public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
			maxTs = Math.max(maxTs, eventTimestamp);
			System.out.println("当前数据发射水位线" + eventTimestamp);
			output.emitWatermark(new Watermark(eventTimestamp));
		}

		//周期性生成水位线
		@Override
		public void onPeriodicEmit(WatermarkOutput output) {
			output.emitWatermark(new Watermark(200));
		}
	}

	// 乱序流生成水位线
	public static class OrderlessWatermark implements WatermarkGenerator<Event> {
		private Long delay = 2000L;
		private Long maxTs = Long.MIN_VALUE + delay;

		//每条数据生成水位线
		@Override
		public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
			maxTs = Math.max(maxTs, eventTimestamp);
//			System.out.println("每条数据生成水位线: " + maxTs);
//			output.emitWatermark(new Watermark(maxTs));

		}

		//周期性生成水位线
		@Override
		public void onPeriodicEmit(WatermarkOutput output) {
			System.out.println("周期性发射水位线" + (maxTs - delay));
			output.emitWatermark(new Watermark(maxTs - delay));
		}
	}

	//有序流周期性水位线生成器
	public static class CycleOrderWatermark implements WatermarkGenerator<Event> {

		private Long maxTs = Long.MIN_VALUE;

		//每条数据生成水位线
		@Override
		public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
			maxTs = Math.max(maxTs, eventTimestamp);
		}

		//周期性生成水位线
		@Override
		public void onPeriodicEmit(WatermarkOutput output) {
			System.out.println("周期性发射水位线" + maxTs);
			output.emitWatermark(new Watermark(maxTs));
		}
	}

}
