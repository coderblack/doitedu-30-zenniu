package rpt_ad;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class AdClickPredictionFeature_HBASE {
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

        // 创建一个hbase的sink表
        tenv.executeSql("CREATE TABLE request_log_sink (\n" +
                " rowkey STRING,\n" +
                " f ROW<device_id STRING, ad_id STRING, ts STRING,f1 int,f2 int,f3 int,f4 int>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED   \n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',  \n" +
                " 'table-name' = 'request_log',  \n" +
                " 'zookeeper.quorum' = 'doit01:2181'  \n" +
                ")");


        // 从kafka表select数据 插入hbase表
        tenv.executeSql("insert into request_log_sink\n" +
                "select\n" +
                "tracking_id,\n" +
                "row(device_id,ad_id,ts,f1,f2,f3,f4) as f\n" +
                "from \n" +
                "(\n" +
                "select\n" +
                "  tracking_id,\n" +
                "  device_id,ad_id,DATE_FORMAT(ts, 'yyyy-MM-dd HH:mm:ss.SSS') as ts,f1,f2,f3,f4\n" +
                "from request_log  \n" +
                ")" );


    }
}
