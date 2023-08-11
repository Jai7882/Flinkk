import com.jia.flink.pojo.Event;
import com.jia.flink.pojo.UrlViewCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.time.Duration;

/**
 * ClassName: Test1
 * Package: PACKAGE_NAME
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/11 8:37
 * @Version 1.0
 */
public class Test1 {

	/*
	1 窗口的种类
	 	  时间窗口
	 	  	滚动窗口
	 	  		滚动处理窗口
	 	  		滚动事件窗口
	 	  	滑动窗口
	 	  		滑动处理窗口
	 	  		滑动事件窗口
	 	  	会话窗口
	 	  		滑动会话窗口
	 	  		滚动会话窗口
	 	  计数窗口
	 	  	计数滑动窗口
	 	  	计数滚动窗口
	 非按键分区窗口
	 	都是xxxAll()

	2 窗口函数的种类
		聚合函数窗口
			计算效率高，但是不包含窗口信息
		全函数窗口
			计算效率不如聚合函数，但是包含窗口信息

	3 flink处理迟到数据
		1 略微迟到数据 通过延迟推进水位线 以等待迟到数据 可以参与计算
		2 一般迟到数据 可以通过window方法后调用allowedLateness()方法延迟窗口关闭以确保数据准确性 可以参与计算
		3 异常迟到数据 可以通过写入测流中 但是不参与数据的计算 通常这部分数据交由开发人员决定如何处理

	 */


	@Test
	public void t1() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
				.setTopics("topic_c")
				.setGroupId("flink")
				.setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
				.build();
		DataStreamSource<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
		SingleOutputStreamOperator<Event> assigned = ds.map(e -> {
					String[] words = e.split(",");
					return new Event(words[0], words[1], Long.valueOf(words[2]));
				})
				.assignTimestampsAndWatermarks(
						WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
								.withTimestampAssigner((e, ts) -> e.getTs())
				);
		SingleOutputStreamOperator<UrlViewCount> urlDs = assigned.map(e -> Tuple2.of(e.getUrl(), 1L))
				.returns(Types.TUPLE(Types.STRING, Types.LONG))
				.keyBy(e -> e.f0)
				.window(TumblingEventTimeWindows.of(Time.seconds(10)))
				.aggregate(
						new AggregateFunction<Tuple2<String, Long>, Long, UrlViewCount>() {
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
								return new UrlViewCount(null, null, null, accumulator);
							}

							@Override
							public Long merge(Long a, Long b) {
								return null;
							}
						},
						new ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>() {
							@Override
							public void process(String key, ProcessWindowFunction<UrlViewCount, UrlViewCount, String, TimeWindow>.Context context, Iterable<UrlViewCount> elements, Collector<UrlViewCount> out) throws Exception {
								UrlViewCount urlViewCount = elements.iterator().next();
								urlViewCount.setUrl(key);
								urlViewCount.setStart(context.window().getStart());
								urlViewCount.setEnd(context.window().getEnd());
								out.collect(urlViewCount);
							}
						}
				);

		SinkFunction<UrlViewCount> sink = JdbcSink.sink(
				"replace into url_count(url,stt,edt,count) values(?,?,?,?)",
				(PreparedStatement ps, UrlViewCount url) -> {
					ps.setString(1, url.getUrl());
					ps.setLong(2, url.getStart());
					ps.setLong(3, url.getEnd());
					ps.setLong(4, url.getCount());
				},
				new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
						.withUrl("jdbc:mysql://hadoop102:3306/t")
						.withUsername("root")
						.withPassword("000000")
						.build()
		);
		urlDs.addSink(sink);

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}
}
