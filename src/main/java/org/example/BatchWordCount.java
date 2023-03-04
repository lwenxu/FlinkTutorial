package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

// 批处理
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        // 执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> source = env.readTextFile("C:\\Users\\lwen\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\word.txt");

        AggregateOperator<Tuple2<String, Integer>> sum = source
                // 尽量不要用lambada表达式，因为类型无法识别会报错
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] words = line.split(" ");
                        for (String word : words) {
                            Tuple2<String, Integer> tuple2 = new Tuple2<>(word, 1);
                            collector.collect(tuple2);
                        }
                    }
                })
                .groupBy(0) //按照位置为 1 的字段进行分组
                .sum(1);// 对第二个位置进行求和

        sum.print();
    }

}
