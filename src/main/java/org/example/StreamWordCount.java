package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //  不同于批的是这里用的 StreamExecutionEnvironment 而不是 ExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(3);
        DataStream<String> source = env.readTextFile("C:\\Users\\lwen\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\word.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = source
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
                .keyBy(0) // 在stream中没有 groupBy 而是通过 keyBy 因为groupBy是对数据重分组意味着所有的数据都到了，而keyBy则是根据字段的hash进行重映射
                .sum(1);// 对第二个位置进行求和

        sum.print();

        // 不同于批处理，必须要执行任务等整个任务在等待状态，直到有数据来
        env.execute();

        // 结果输出分析，最终的结果如下
        //        10> (flink,1)
        //        7> (fine,1)
        //        4> (hello,1)
        //        1> (spark,1)
        //        1> (scala,1)
        //        4> (hello,2)
        //        4> (hello,3)

        // 可以看到前面的有一个编号，这个是在本地模拟分布式环境下的多机器执行，这里通过了多线程来模拟的
        // 前面的编号就是线程号，编号目前最大是 10 也就是开启了是个线程来跑，我们可以在 env 上设置并行度
        // 另外可以看到 hello 这个词出现了三次，这意味着底层是一个流式处理的逻辑，他是有状态的叠加的
    }
}
