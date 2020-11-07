package myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ClassName SocketWindowCount
 * @Description 从服务器接受数据计算单词数
 * @Author 中森明菜
 * @Date 2020/11/6 15:20
 * @Version 1.0
 */
public class SocketWindowCount {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment ev = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置数据源 监听9999端口号的数据
        DataStreamSource<String> source = ev.socketTextStream("202.61.130.224", 9999, "\n");

        // keyBy 以元组的第一个参数分组 这里是单词
        // timeWindow 每5秒处理一次数据
        // sum 对元组的第二个参数求和
        DataStream<Tuple2<String, Integer>> dataStream = source.flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        // 打印在控制台
        dataStream.print();

        // 提交执行
        ev.execute("Window WordCount");
    }

    // 将每行数据 以空格分割 初始化单词数量
    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word : value.split(" ")) {
                collector.collect(Tuple2.of(word, 1));
            }
        }
    }
}
