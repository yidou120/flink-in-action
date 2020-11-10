package myflink;

import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


/**
 * @ClassName HotItems
 * @Description 实时计算一小时内点击量排名前三的商品
 * @Author 中森明菜
 * @Date 2020/11/8 12:25
 * @Version 1.0
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        PojoTypeInfo<UserBehavior> typeInfo = (PojoTypeInfo<UserBehavior>)TypeExtractor.createTypeInfo(UserBehavior.class);

        URL url = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        File file = new File(url.toURI());
        Path path = Path.fromLocalFile(file);

        // 设置pojo的字段顺序
        String [] fieldNames = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        PojoCsvInputFormat<UserBehavior> csvInputFormat = new PojoCsvInputFormat<>(path, typeInfo, fieldNames);
        // 设置数据源
        DataStreamSource<UserBehavior> streamSource = env.createInput(csvInputFormat, typeInfo);

        // 设置EventTime模式 事件处理的时间 一般就是数据自带的时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 标记数据流
        SingleOutputStreamOperator<UserBehavior> watermarks = streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(new TimestampAssignerSupplier<UserBehavior>() {
            @Override
            public TimestampAssigner<UserBehavior> createTimestampAssigner(Context context) {
                return new timeStamp();
            }
        }));
        // 过滤点击事件
        SingleOutputStreamOperator<UserBehavior> filterData = watermarks.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return value.behavior.equals("pv");
            }
        });

        // 设置时间窗口和滑动时间 统计点击量
        SingleOutputStreamOperator<ItemViewCount> windowedData = filterData.keyBy(behavior -> behavior.itemId)
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new AggregateFunction<UserBehavior, Long, Long>() {

                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(UserBehavior value, Long accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                }, new WindowFunction<Long, ItemViewCount, Long, TimeWindow>() {

                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
                        Long itemId = aLong;
                        Long viewCount = input.iterator().next();
                        out.collect(ItemViewCount.of(itemId, window.getEnd(), viewCount));
                    }
                });

        // 求topN
        SingleOutputStreamOperator<String> topNData = windowedData.keyBy(item -> item.windowEnd)
                .process(new ItemsTopN(3));
        topNData.print();
        env.setParallelism(1);
        env.execute("Hot items Job");
    }

    public static class timeStamp implements TimestampAssigner<UserBehavior>{
        @Override
        public long extractTimestamp(UserBehavior element, long recordTimestamp) {
            return element.timestamp * 1000;
        }
    }

    /** 求topN的商品 **/
    public static class ItemsTopN extends KeyedProcessFunction<Long, ItemViewCount, String>{

        private int topN = 0;

        private ListState<ItemViewCount> itemState;

        public ItemsTopN(int topN) {
            this.topN = topN;
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount itemViewCount : itemState.get()) {
                allItems.add(itemViewCount);
            }
            itemState.clear();
            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o2.viewCount - o1.viewCount);
                }
            });
            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append(new Timestamp(timestamp-1)).append("\n");
            for (int i=0;i<topN;i++){
                ItemViewCount currentItem = allItems.get(i);
                result.append("No").append(i).append(":")
                        .append("  商品ID=").append(currentItem.itemId)
                        .append("  浏览量=").append(currentItem.viewCount)
                        .append("\n");
            }
            result.append("====================================\n\n");
            out.collect(result.toString());
            super.onTimer(timestamp, ctx, out);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<ItemViewCount> listStateDescriptor = new ListStateDescriptor<ItemViewCount>("itemState-state", ItemViewCount.class);
            itemState = getRuntimeContext().getListState(listStateDescriptor);
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            itemState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
        }
    }

    /** 用户行为数据结构 **/
    public static class UserBehavior {
        public long userId;         // 用户ID
        public long itemId;         // 商品ID
        public int categoryId;      // 商品类目ID
        public String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav")
        public long timestamp;      // 行为发生的时间戳，单位秒
    }

    /** 商品点击量(窗口操作的输出类型) **/
    public static class ItemViewCount {
        public long itemId;     // 商品ID
        public long windowEnd;  // 窗口结束时间戳
        public long viewCount;  // 商品的点击量

        public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
            ItemViewCount result = new ItemViewCount();
            result.itemId = itemId;
            result.windowEnd = windowEnd;
            result.viewCount = viewCount;
            return result;
        }
    }
}
