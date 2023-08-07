package com.jia.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * ClassName: BatchWordCount
 * Package: com.jia.flink
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/3 16:16
 * @Version 1.0
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.readTextFile("Flink/input/test.txt");
        FlatMapOperator<String, Tuple2<String, Long>> flatMap = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] split = line.split(" ");
                for (String word : split) {
                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        });
        UnsortedGrouping<Tuple2<String, Long>> groupBy = flatMap.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> sum = groupBy.sum(1);
        sum.print();

    }


}
