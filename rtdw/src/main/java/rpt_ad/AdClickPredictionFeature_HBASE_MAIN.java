package rpt_ad;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.time.LocalDateTime;

public class AdClickPredictionFeature_HBASE_MAIN {
    public static void main(String[] args) throws Exception {

        /* *
         * topic :  request-log
         *  假如有一下  广告请求日志流（广告引擎产生的，带广告特征和受众特征）
         *  tr01,dev01,ad01,2022-06-27 15:20:00,1,3,10,50
         *  tr02,dev02,ad02,2022-06-27 15:21:40,2,5,20,30
         *  tr03,dev03,ad01,2022-06-27 15:22:20,8,6,15,40
         *  tr04,dev04,ad02,2022-06-27 15:23:50,4,3,10,20
         */


        /* *
         * topic : ad-events
         * 假如有如下 曝光点击事件流
         * tr01,dev01,ad01,adshow,2022-06-27 15:20:30
         * tr01,dev01,ad01,adclick,2022-06-27 15:21:30
         * tr02,dev02,ad02,adshow,2022-06-27 15:22:00
         * tr03,dev03,ad01,adshow,2022-06-27 15:23:00
         * tr04,dev04,ad02,adshow,2022-06-27 15:24:00
         * tr04,dev04,ad02,adclick,2022-06-27 15:25:00
         */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        tenv.executeSql(
                " create  table ad_events_log(                          " +
                        "   tracking_id   string,                             " +
                        "   device_id     string,                             " +
                        "   ad_id         string,                             " +
                        "   event_id      string,                             " +
                        "   ts            timestamp(3),                       " +
                        "   rt as ts   ,                                      " +
                        "   watermark for rt as rt                            " +
                        " ) WITH (                                            " +
                        "   'connector' = 'kafka',                            " +
                        "   'topic' = 'ad-events',                            " +
                        "   'properties.bootstrap.servers' = 'doit01:9092',   " +
                        "   'properties.group.id' = 'g2',                     " +
                        "   'scan.startup.mode' = 'earliest-offset',          " +
                        "   'format' = 'csv'                                  " +
                        " )                                                   "
        );


        Table validShow = tenv.sqlQuery("SELECT  *       " +
                "FROM ad_events_log     " +
                "    MATCH_RECOGNIZE (     " +
                "        PARTITION BY tracking_id     " +
                "        ORDER BY rt     " +
                "        MEASURES     " +
                "            A.device_id AS device_id,     " +
                "            A.ad_id AS ad_id,     " +
                "            A.event_id AS event_id  ,  " +
                "            A.ts AS show_ts  ,  " +
                "            B.ts AS click_ts   " +
                "        ONE ROW PER MATCH     " +
                "        AFTER MATCH SKIP TO LAST B     " +
                "        PATTERN (A B)     " +
                "        DEFINE   " +
                "            A AS  A.event_id = 'adshow'  ,  " +
                "            B AS  B.event_id = 'adclick'    " +
                "    ) MR");


        DataStream<Row> validShowDataStream = tenv.toDataStream(validShow);
        validShowDataStream.map(row -> {
                    String tracking_id = row.getFieldAs("tracking_id");
                    String device_id = row.getFieldAs("device_id");
                    String ad_id = row.getFieldAs("ad_id");
                    String event_id = row.getFieldAs("event_id");
                    LocalDateTime show_ts = row.getFieldAs("show_ts");
                    LocalDateTime click_ts = row.getFieldAs("click_ts");
                    return new ValidShowBean(tracking_id, device_id, ad_id, event_id, show_ts, click_ts,0);
                })
                .keyBy(bean -> bean.getTracking_id())
                .process(new KeyedProcessFunction<String, ValidShowBean, String>() {

                    Connection conn;
                    org.apache.hadoop.hbase.client.Table table;
                    MapState<Long, ValidShowBean> showBeanMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
                        conf.set("hbase.zookeeper.quorum","doit01:2181,doit02:2181");
                        conn = ConnectionFactory.createConnection(conf);
                        table = conn.getTable(TableName.valueOf("request_log"));


                        showBeanMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, ValidShowBean>("x", Long.class, ValidShowBean.class));


                    }

                    @Override
                    public void processElement(ValidShowBean showBean, KeyedProcessFunction<String, ValidShowBean, String>.Context ctx, Collector<String> out) throws Exception {

                        Result result = table.get(new Get(showBean.getTracking_id().getBytes()));
                        if(!result.isEmpty()) {
                            // device_id STRING, ad_id STRING, ts STRING,f1 int,f2 int,f3 int,f4 int
                            String deviceId = new String(result.getValue("f".getBytes(), "device_id".getBytes()));
                            String adId = new String(result.getValue("f".getBytes(), "ad_id".getBytes()));
                            String ts = new String(result.getValue("f".getBytes(), "ts".getBytes()));
                            int f1 = Bytes.toInt(result.getValue("f".getBytes(), "f1".getBytes()));
                            int f2 = Bytes.toInt(result.getValue("f".getBytes(), "f2".getBytes()));
                            int f3 = Bytes.toInt(result.getValue("f".getBytes(), "f3".getBytes()));
                            int f4 = Bytes.toInt(result.getValue("f".getBytes(), "f4".getBytes()));

                            out.collect(String.format("%s | %s , %s , %s ,%d ,%d ,%d ,%d",showBean.toString(),deviceId,adId,ts,f1,f2,f3,f4));
                        }else{
                            long processingTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(processingTime+5*1000);


                            showBean.setQueryAttempts(1);
                            showBeanMapState.put(processingTime+5*1000,showBean);
                        }
                    }


                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, ValidShowBean, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

                        ValidShowBean validShowBean = showBeanMapState.get(ctx.timestamp());
                        showBeanMapState.remove(timestamp);

                        Result result = table.get(new Get(validShowBean.getTracking_id().getBytes()));
                        if(!result.isEmpty()) {
                            // device_id STRING, ad_id STRING, ts STRING,f1 int,f2 int,f3 int,f4 int
                            String deviceId = new String(result.getValue("f".getBytes(), "device_id".getBytes()));
                            String adId = new String(result.getValue("f".getBytes(), "ad_id".getBytes()));
                            String ts = new String(result.getValue("f".getBytes(), "ts".getBytes()));
                            int f1 = Bytes.toInt(result.getValue("f".getBytes(), "f1".getBytes()));
                            int f2 = Bytes.toInt(result.getValue("f".getBytes(), "f2".getBytes()));
                            int f3 = Bytes.toInt(result.getValue("f".getBytes(), "f3".getBytes()));
                            int f4 = Bytes.toInt(result.getValue("f".getBytes(), "f4".getBytes()));

                            out.collect(String.format("%s | %s , %s , %s ,%d ,%d ,%d ,%d",validShowBean,deviceId,adId,ts,f1,f2,f3,f4));

                        }else{
                            if(validShowBean.getQueryAttempts() < 3 ) {
                                long processingTime = ctx.timerService().currentProcessingTime();
                                ctx.timerService().registerProcessingTimeTimer(processingTime + 5 * 1000);
                                validShowBean.setQueryAttempts(validShowBean.getQueryAttempts()+1);
                                showBeanMapState.put(processingTime + 5 * 1000, validShowBean);

                            }
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        table.close();
                        conn.close();
                    }
                }).print();


        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ValidShowBean {
        private String tracking_id;
        private String device_id;
        private String ad_id;
        private String event_id;
        private LocalDateTime show_ts;
        private LocalDateTime click_ts;
        private int queryAttempts;
    }


}
