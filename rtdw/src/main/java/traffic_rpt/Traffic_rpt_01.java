package traffic_rpt;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import pojo.EventBean;

public class Traffic_rpt_01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // 从kafka的 dwd-mall-app-log
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setGroupId("doe-001")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("dwd-mall-app-log2")
                .build();


        DataStreamSource<String> ds1 = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "dwd-mall-app-log");
        SingleOutputStreamOperator<EventBean> ds2 = ds1.map(s -> JSON.parseObject(s, EventBean.class));


        SingleOutputStreamOperator<EventBean> virtualed = ds2.keyBy(EventBean::getGuid)
                .process(new KeyedProcessFunction<Long, EventBean, EventBean>() {

                    ValueState<EventBean> pageStartBeanState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        pageStartBeanState = getRuntimeContext().getState(new ValueStateDescriptor<EventBean>("page_start_bean", EventBean.class));
                    }

                    @Override
                    public void processElement(EventBean eventBean, KeyedProcessFunction<Long, EventBean, EventBean>.Context ctx, Collector<EventBean> out) throws Exception {

                        // 如果是一个app启动事件，则要记录这条bean到状态中，并生成页面起始时间
                        if (eventBean.getEventid().equals("launch")) {
                            eventBean.setStartPageId(eventBean.getProperties().get("pageId"));
                            eventBean.setPageStartTime(eventBean.getTimestamp());
                            eventBean.setNewSessionId(eventBean.getSessionid() + "-" + eventBean.getTimestamp());
                            // 设置一下页面起始时间

                            pageStartBeanState.update(eventBean);
                        } else if (eventBean.getEventid().equals("pageload")) {
                            // 如果是一个新的页面加载事件，则先输出一条虚拟事件： 事件时间=本条事件的时间，事件的起始页=上一条起始页 ，事件的起始页加载时间=上一条起始页加载时间
                            EventBean virtualEvent = new EventBean();
                            virtualEvent.setTimestamp(eventBean.getTimestamp());
                            virtualEvent.setStartPageId(pageStartBeanState.value().getStartPageId());
                            virtualEvent.setPageStartTime(pageStartBeanState.value().getPageStartTime());
                            virtualEvent.setNewSessionId(pageStartBeanState.value().getNewSessionId());
                            out.collect(virtualEvent);

                            // 将本次pageload的信息，更新到状态中
                            eventBean.setStartPageId(eventBean.getProperties().get("pageId"));
                            eventBean.setPageStartTime(eventBean.getTimestamp());
                            eventBean.setNewSessionId((pageStartBeanState.value().getNewSessionId()));
                            pageStartBeanState.update(eventBean);

                        } else if (eventBean.getEventid().equals("wakeup")) {
                            eventBean.setStartPageId(eventBean.getProperties().get("pageId"));
                            eventBean.setPageStartTime(eventBean.getTimestamp());
                            eventBean.setNewSessionId(eventBean.getSessionid() + "-" + eventBean.getTimestamp());
                            pageStartBeanState.update(eventBean);
                        } else {

                            eventBean.setStartPageId(pageStartBeanState.value().getStartPageId());
                            eventBean.setPageStartTime(pageStartBeanState.value().getPageStartTime());
                            eventBean.setNewSessionId(pageStartBeanState.value().getNewSessionId());

                        }

                        out.collect(eventBean);
                    }
                });


        tenv.createTemporaryView("traffic_detail", virtualed, Schema.newBuilder()
                .column("account", DataTypes.STRING())
                .column("appid", DataTypes.STRING())
                .column("appversion", DataTypes.STRING())
                .column("carrier", DataTypes.STRING())
                .column("deviceid", DataTypes.STRING())
                .column("devicetype", DataTypes.STRING())
                .column("eventid", DataTypes.STRING())
                .column("ip", DataTypes.STRING())
                .column("latitude", DataTypes.DOUBLE())
                .column("longitude", DataTypes.DOUBLE())
                .column("nettype", DataTypes.STRING())
                .column("osname", DataTypes.STRING())
                .column("osversion", DataTypes.STRING())
                .column("properties", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                .column("releasechannel", DataTypes.STRING())
                .column("resolution", DataTypes.STRING())
                .column("sessionid", DataTypes.STRING())
                .column("timestamp", DataTypes.BIGINT())
                .column("guid", DataTypes.BIGINT())
                .column("registerTime", DataTypes.BIGINT())
                .column("firstAccessTime", DataTypes.BIGINT())
                .column("isNew", DataTypes.INT())
                .column("geoHashCode", DataTypes.STRING())
                .column("province", DataTypes.STRING())
                .column("city", DataTypes.STRING())
                .column("region", DataTypes.STRING())
                .column("pageStartTime", DataTypes.BIGINT())
                .column("startPageId", DataTypes.STRING())
                .column("newSessionId", DataTypes.STRING())
                .columnByExpression("rt", "to_timestamp_ltz(`timestamp`,3)")
                .watermark("rt", "rt - interval '0' second ")
                .build());


        /**
         * 报表1： 每分钟统计一次最近30分钟内的  每个用户的pv数、页面访问总时长、会话数、会话时长
         */
        tenv.executeSql("SELECT\n" +
                " window_start,\n" +
                " window_end,\n" +
                " guid,\n" +
                " count(1) as pv,\n" +
                " sum(pageStaylong) as pageStaylong_amt,\n" +
                " count(distinct newSessionId) as session_cnt,\n" +
                " sum(pageStaylong) as session_timelong\n" +
                "  \n" +
                "FROM\n" +
                "(\n" +
                "  SELECT\n" +
                "    window_start,\n" +
                "    window_end,\n" +
                "    guid, \n" +
                "    sessionid,\n" +
                "    newSessionId,\n" +
                "    startPageId,\n" +
                "    pageStartTime,\n" +
                "    max(`timestamp`) - min(`timestamp`) as pageStaylong  -- 页面停留时长\n" +
                "  FROM table(hop(table traffic_detail, descriptor(rt) ,interval '1' minute, interval '30' minute  ) )\n" +
                "  GROUP BY window_start,window_end,guid,sessionid,newSessionId,startPageId,pageStartTime\n" +
                ")\n" +
                "GROUP BY guid,window_start,window_end").print();


        /**
         * 报表2： 每分钟统计一次当天累计到当前的，每个用户的pv数、页面访问总时长、会话数、会话时长
         */
        tenv.executeSql("SELECT\n" +
                " window_start,\n" +
                " window_end,\n" +
                " guid,\n" +
                " count(1) as pv,\n" +
                " sum(pageStaylong) as pageStaylong_amt,\n" +
                " count(distinct newsessionid) as session_cnt,\n" +
                " sum(pageStaylong) as session_timelong\n" +
                "  \n" +
                "FROM\n" +
                "(\n" +
                "  SELECT\n" +
                "    window_start,\n" +
                "\twindow_end,\n" +
                "    guid, \n" +
                "    sessionid,\n" +
                "    newsessionid,\n" +
                "    startpageid,\n" +
                "    pageStartTime,\n" +
                "    max(`timestamp`) - min(`timestamp`) as pageStaylong  -- 页面停留时长\n" +
                "  FROM table(CUMULATE(table traffic_detail, descriptor(rt) ,interval '1' minute, interval '24' hour  ) )\n" +
                "  GROUP BY window_start,window_end,guid,sessionid,newsessionid,startpageid,pageStartTime\n" +
                ")\n" +
                "GROUP BY GUID,window_start,window_end");


        /**
         * 报表3： 每分钟统计一次当天累计到当前的，每个省的用户日活数，日新数，用户总访问时长，用户总会话个数，总pv数
         */


        env.execute();


    }
}
