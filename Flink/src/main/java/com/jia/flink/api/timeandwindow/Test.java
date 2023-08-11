package com.jia.flink.api.timeandwindow;

import com.alibaba.fastjson.JSON;
import com.google.common.primitives.Bytes;
import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * ClassName: Test
 * Package: com.jia.flink.api.timeandwindow
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/10 8:35
 * @Version 1.0
 */
public class Test {

	// 1 flink中有两种时间语义 分别是 事件时间 和 处理时间   事件时间关注的是事件发生的时间关键在于数据完整性 处理时间关注的是处理时的时间 关注的是处理效率

	// 2 水位线本质上就是时间戳，记录了事件的随时间推进的记录 水位线只能是单调递增的

	// 3 水位线 滑动 滚动两种

	// 4 水位线传递原则是 上游传递给下游是采用广播的方式 不同的上游给下游传递水位线取时间戳最低的那条


	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
				.setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
				.setGroupId("flink")
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.setTopics("topic_b")
				.build();
		SingleOutputStreamOperator<Tuple2<String, Long>> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")
				.flatMap(
						((String s, Collector<Tuple2<String, Long>> out) -> {
							String[] words = s.split(",");
							out.collect(Tuple2.of(words[2].trim(), 1L));
						})
				).returns(Types.TUPLE(Types.STRING, Types.LONG));
		SingleOutputStreamOperator<Tuple2<String, Long>> sum = ds.keyBy(v -> v.f0).sum(1);
		sum.print("sum");
		KafkaSink<Tuple2<String, Long>> kafkaSink = KafkaSink.<Tuple2<String, Long>>builder().setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
				.setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<String, Long>>() {
					@Nullable
					@Override
					public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Long> v, KafkaSinkContext kafkaSinkContext, Long aLong) {
						return new ProducerRecord<>("topicC", v.f0.getBytes(), v.f1.toString().getBytes());
					}
				})
				.build();
		sum.sinkTo(kafkaSink);
		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


}
