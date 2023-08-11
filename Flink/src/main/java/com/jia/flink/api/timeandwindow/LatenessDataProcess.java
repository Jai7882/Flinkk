package com.jia.flink.api.timeandwindow;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * ClassName: LatenessDataProcess
 * Package: com.jia.flink.api.timeandwindow
 * Description:
 *
 *
 *  Flink对迟到数据的处理
 *    1 推迟水位线 可以考虑到大部分数据的延迟情况
 *    2 延迟窗口的关闭 让迟到更久的数据也可以正确的参与到窗口的计算
 *    3 极少数的极端迟到数据 flink会将这部分数据通过测流输出 该部分数据的后续处理交给开发人员决定
 *
 */
public class LatenessDataProcess {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		SingleOutputStreamOperator<Event> ds = env.socketTextStream("hadoop102", 8888)
				.map(e -> {
					String[] words = e.split(",");
					return new Event(words[0].trim(), words[1].trim(), Long.valueOf(words[2].trim()));
				})
				.assignTimestampsAndWatermarks(
						WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)) // 水位线延迟两秒
								.withTimestampAssigner((e, ts) -> e.getTs())
				);
		ds.print("input");
		OutputTag<Tuple2<String,Long>> outputTag = new OutputTag<>("late",Types.TUPLE(Types.STRING,Types.LONG));

		// 统计每个用户每10s的点击次数
		SingleOutputStreamOperator<Tuple2<String, Long>> windowDs = ds.map(e -> Tuple2.of(e.getUser(), 1L))
				.returns(Types.TUPLE(Types.STRING, Types.LONG))
				.keyBy(e -> e.f0)
				.window(TumblingEventTimeWindows.of(Time.seconds(10)))
				.allowedLateness(Time.seconds(10)) //窗口延时10s关闭
				.sideOutputLateData(outputTag) //将极端迟到数据输出到测流
				.sum(1);
		windowDs.print();
		// 提取测流的迟到数据
		SideOutputDataStream<Tuple2<String, Long>> lateDs = windowDs.getSideOutput(outputTag);
		lateDs.print("迟到数据"); // 该部分数据处理由开发人员决定

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
