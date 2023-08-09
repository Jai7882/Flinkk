package com.jia.flink.api.sink;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * ClassName: JDBCSinkTest
 * Package: com.jia.flink.api.sink
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/9 9:40
 * @Version 1.0
 */
public class JDBCSinkTest {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Event> ds = env.addSource(new ClickSource());

		SinkFunction<Event> jdbcSink = JdbcSink.sink(
				//幂等写 防止主键重复
				"replace into clicks values(?,?,?)",
				(PreparedStatement statement, Event event) -> {
					statement.setString(1, event.getUser());
					statement.setString(2, event.getUrl());
					statement.setLong(3, event.getTs());
				},
				JdbcExecutionOptions.builder()
						.withBatchIntervalMs(5000)
						.withBatchSize(5)
						.withMaxRetries(5)
						.build(),
				new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
						.withConnectionCheckTimeoutSeconds(5)
						.withPassword("000000")
						.withUsername("root")
						.withUrl("jdbc:mysql://hadoop102:3306/gmall")
						.build()
		);

		ds.addSink(jdbcSink);

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
