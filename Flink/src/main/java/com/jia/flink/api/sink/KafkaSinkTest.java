package com.jia.flink.api.sink;

import com.alibaba.fastjson.JSON;
import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: KafkaSinkTest
 * Package: com.jia.flink.api.sink
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/8 17:10
 * @Version 1.0
 */
public class KafkaSinkTest {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(2000);
		SingleOutputStreamOperator<String> ds = env.addSource(new ClickSource()).map(JSON::toJSONString);
		KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
				.setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
				.setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
						.setTopic("topic_a")
						.setValueSerializationSchema(new SimpleStringSchema())
						.build())
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.setProperty("transaction.timeout.ms", "10000")
				.setTransactionalIdPrefix("flink"+ RandomUtils.nextInt(1,100))
				.build();

		ds.sinkTo(kafkaSink);

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


}
