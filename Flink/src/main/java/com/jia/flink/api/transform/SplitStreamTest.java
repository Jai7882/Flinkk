package com.jia.flink.api.transform;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * ClassName: SplitStreamTest
 * Package: com.jia.flink.api.transform
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 19:32
 * @Version 1.0
 */
public class SplitStreamTest {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Event> ds = env.addSource(new ClickSource());
		SingleOutputStreamOperator<Event> ds1 = ds.filter(event -> "Zhang3".equals(event.getUser()) || "Li4".equals(event.getUser()));
		ds1.print("ds1");
		SingleOutputStreamOperator<Event> ds2 = ds.filter(event -> "Tom".equals(event.getUser()) || "Jerry".equals(event.getUser()));
		ds2.print("ds2");

		OutputTag<Event> ds1Tag = new OutputTag<Event>("ds1"){};
		OutputTag<Event> ds2Tag = new OutputTag<Event>("ds2"){};

		SingleOutputStreamOperator<Event> mainDs = ds.process(
				new ProcessFunction<Event, Event>() {
					@Override
					public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
						if ("Zhang3".equals(value.getUser()) || "Li4".equals(value.getUser())) {
							//ds1
							ctx.output(ds1Tag, value);
						} else if ("Tom".equals(value.getUser()) || "Jerry".equals(value.getUser())) {
							ctx.output(ds2Tag, value);
							//ds2
						} else {
							//side
							out.collect(value);
						}
					}
				}
		);
		mainDs.print("mainSide");
		SideOutputDataStream<Event> ds1P = mainDs.getSideOutput(ds1Tag);
		SideOutputDataStream<Event> ds2P = mainDs.getSideOutput(ds2Tag);

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
