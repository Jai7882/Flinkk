package com.jia.flink.wordcount;

import com.jia.flink.pojo.WordCount;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: POJOWordCount
 * Package: com.jia.flink.wordcount
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/4 10:23
 * @Version 1.0
 */
public class POJOWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置执行模式
        // STREAMING 流模式 默认值
        // BATCH     批处理
        // AUTOMATIC 自动选择 根据数据源是否有界 有界批处理 无界流处理
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);
        SingleOutputStreamOperator<WordCount> flatMap = ds.flatMap((line, collector)->{
            String[] words = line.split(" ");
            for (String word : words) {
                collector.collect(new WordCount(word,1L));
            }
        });
        flatMap.returns(WordCount.class);

        KeyedStream<WordCount, String> keyBy = flatMap.keyBy(WordCount::getWord);

        SingleOutputStreamOperator<WordCount> sum = keyBy.sum("count");

        sum.print();
        env.execute();
    }
}
