package com.jia.flink.api.sink;

import com.alibaba.fastjson.JSON;
import com.jia.flink.pojo.Event;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * ClassName: MySinkFunction
 * Package: com.jia.flink.api.sink
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/9 10:11
 * @Version 1.0
 */
public class MySinkFunction implements SinkFunction<Event> {
	@Override
	public void invoke(Event value, Context context) throws Exception {
		// 实现数据写出 写到各种目标数据库
		System.out.println(JSON.toJSONString(value));

	}
}
