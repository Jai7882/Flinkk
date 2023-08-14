package com.jia.flink.state;

import com.alibaba.fastjson.JSON;
import com.jia.flink.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * ClassName: TestState
 * Package: com.jia.flink.state
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/12 8:36
 * @Version 1.0
 */
public class TestState {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		MapStateDescriptor<String, String> conf = new MapStateDescriptor<>("conf", Types.STRING, Types.STRING);
		BroadcastStream<String> confDs = env.socketTextStream("hadoop102", 8888)
				.broadcast(conf);
		KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
				.setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
				.setTopics("topic1")
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();
		KeyedStream<WaterSensor, String> dataDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")
				.map(e -> {
					String[] words = e.split(",");
					return new WaterSensor(words[0].trim(), Integer.valueOf(words[1].trim()), Long.valueOf(words[2].trim()));
				})
				.keyBy(WaterSensor::getId);
		OutputTag<String> tag = new OutputTag<>("tag", Types.STRING);
		SingleOutputStreamOperator<String> process = dataDs.connect(confDs)
				.process(
						new KeyedBroadcastProcessFunction<String, WaterSensor, String, String>() {
							@Override
							public void processElement(WaterSensor value, KeyedBroadcastProcessFunction<String, WaterSensor, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
								ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(conf);
								int vcLimit = Integer.parseInt(broadcastState.get("vcLimit"));
								if (Math.abs(vcLimit - value.getVc()) < vcLimit) {
									out.collect(JSON.toJSONString(value));
								}else {
									ctx.output(tag,value+"超过阈值 , 阈值为:" + vcLimit);
								}
							}

							@Override
							public void processBroadcastElement(String value, KeyedBroadcastProcessFunction<String, WaterSensor, String, String>.Context ctx, Collector<String> out) throws Exception {
								BroadcastState<String, String> broadcastState = ctx.getBroadcastState(conf);
								broadcastState.put("vcLimit", value == null ? "10" : value);
							}
						}
				);
		process.getSideOutput(tag).print("测流输出");
		KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
				.setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
				.setRecordSerializer(
						KafkaRecordSerializationSchema.<String>builder()
								.setTopic("topic2")
								.setValueSerializationSchema(new SimpleStringSchema())
								.build())
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();
		process.sinkTo(kafkaSink);

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


}
