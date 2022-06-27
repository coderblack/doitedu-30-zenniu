package rpt_ad;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class AdClickPredictionFeature_JOIN {
    public static void main(String[] args) {

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
                " create  table request_log(                         "+
                        "   tracking_id   string,                             "+
                        "   device_id     string,                             "+
                        "   ad_id         string,                             "+
                        "   ts            timestamp(3),                       "+
                        "   f1            int,                                "+
                        "   f2            int,                                "+
                        "   f3            int,                                "+
                        "   f4            int,                                "+
                        "   watermark for ts as ts                            "+
                        " ) WITH (                                            "+
                        "   'connector' = 'kafka',                            "+
                        "   'topic' = 'request-log',                          "+
                        "   'properties.bootstrap.servers' = 'doit01:9092',   "+
                        "   'properties.group.id' = 'g1',                     "+
                        "   'scan.startup.mode' = 'earliest-offset',          "+
                        "   'format' = 'csv'                                  "+
                        " )                                                   "
        );


        tenv.executeSql(
                " create  table ad_events_log(                          "+
                        "   tracking_id   string,                             "+
                        "   device_id     string,                             "+
                        "   ad_id         string,                             "+
                        "   event_id      string,                             "+
                        "   ts            timestamp(3),                       "+
                        "   rt as ts   ,                                      "+
                        "   watermark for rt as rt                            "+
                        " ) WITH (                                            "+
                        "   'connector' = 'kafka',                            "+
                        "   'topic' = 'ad-events',                            "+
                        "   'properties.bootstrap.servers' = 'doit01:9092',   "+
                        "   'properties.group.id' = 'g2',                     "+
                        "   'scan.startup.mode' = 'earliest-offset',          "+
                        "   'format' = 'csv'                                  "+
                        " )                                                   "
        );


        //tenv.executeSql("select * from request_log").print();
        //tenv.executeSql("select * from ad_events_log").print();

        tenv.executeSql(" create temporary view valid_show as  SELECT  *       " +
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
                "            A AS  A.event_id = 'adshow'  ,       " +
                "            B AS  B.event_id = 'adclick'    " +
                "    ) MR")/*.print()*/;


        tenv.executeSql("select a.*,b.*  from valid_show a join request_log b on a.tracking_id = b.tracking_id").print();
    }
}
