package com.jia.flink.api.function;

import com.jia.flink.pojo.Event;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * ClassName: ClickSource
 * Package: com.jia.flink.api.function
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 14:45
 * @Version 1.0
 * <p>
 * 使用sourceFunction的方式
 * 1 实现SourceFunction接口
 * 2 重写方法
 */
public class ClickSource implements SourceFunction<Event> {

	static boolean isRunning = true;

	/*
	用于生成数据
	 */
	@Override
	public void run(SourceContext<Event> context) throws Exception {
		//每秒生成一条数据
		while (isRunning){
			// 生成的数据
			String[] users = {"zhang3","li4","wang5","zhao6","tian7"};
			String[] urls = {"/home","/pay","/index","/detail","/cart"};
			Event event = new Event(users[RandomUtils.nextInt(0, 4)],urls[RandomUtils.nextInt(0,4)],System.currentTimeMillis());

			//发射数据
			context.collect(event);
			TimeUnit.SECONDS.sleep(1);
		}

	}

	/*
	用于退出
	 */
	@Override
	public void cancel() {
		isRunning = false;
	}
}
