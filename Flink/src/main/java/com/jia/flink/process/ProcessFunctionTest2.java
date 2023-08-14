package com.jia.flink.process;

/**
 * ClassName: ProcessFunctionTest2
 * Package: com.jia.flink.process
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/11 9:44
 * @Version 1.0
 */
public class ProcessFunctionTest2 {

	/**
	 * ProcessFunction 拥有富函数的方法 生命周期  状态编程
	 *   processElement() 对数据进行各种处理
	 *   onTimer() 定时器触发后 要执行的逻辑
	 *   TimerService 定时器服务 用于定时器编程
	 *   output() 使用测流输出
	 *
	 *   处理函数分类
	 *    1 ProcessFunction -> DataStream.process()
	 *    2 KeyedProcessFunction -> KeyedStream.process()
	 *    3 ProcessWindowFunction -> WindowedStream.process()
	 *    4 ProcessAllWindowFunction -> AllWindowedStream.process()
	 *    5 CoProcessFunction -> ConnectedStream.process()
	 *	  6 ProcessJoinFunction -> IntervalJoined.process()
	 *	  7 BroadcastProcessFunction -> BroadcastConnectedStream()
	 *    8 KeyedBroadcastProcessFunction -> BroadcastConnectedStream()
	 *
 	 */



}
