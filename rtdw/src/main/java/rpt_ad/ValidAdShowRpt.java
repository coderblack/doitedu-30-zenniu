package rpt_ad;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/27
 * @Desc: 学大数据，到多易教育
 *   有效广告曝光数统计
 **/
public class ValidAdShowRpt {
    public static void main(String[] args) {

        // t01,ad01,device01,"2022-06-26 10:00:00",adshow

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        tenv.executeSql("create table ad_event(    " +
                "    tracking_id   string,    " +
                "    ad_id     string,        " +
                "    device_id  string,       " +
                "    event_time timestamp(3), " +
                "    event_id  string ,       " +
                "    watermark for event_time as event_time    " +
                ") with (                          " +
                "   'connector' = 'filesystem',    " +
                "   'path' = 'data/cep/ad.txt',    " +
                "   'format' = 'csv'    " +
                ")");

        // tenv.executeSql("select * from ad_event").print();

        tenv.executeSql("create temporary view  valid_show as " +
                "SELECT *  \n" +
                "FROM ad_event\n" +
                "    MATCH_RECOGNIZE (\n" +
                "        PARTITION BY tracking_id\n" +
                "        ORDER BY event_time\n" +
                "        MEASURES\n" +
                "            A.device_id AS device_id,\n" +
                "            A.ad_id AS ad_id,\n" +
                "            A.event_time AS event_time,\n" +
                "            A.event_id AS event_id\n" +
                "        ONE ROW PER MATCH\n" +
                "        AFTER MATCH SKIP TO LAST B\n" +
                "        PATTERN (A C* B)\n" +
                "        DEFINE    " +
                "            A AS  A.event_id = 'adshow'   ,  \n" +
                "            B AS  B.event_id = 'adshowheart' OR B.event_id = 'adclick' ,\n" +
                "            C AS  C.event_id = 'other' OR C.event_id = 'other2' \n" +
                "    ) MR");

        tenv.executeSql("select ad_id,count(1) as valid_show_cnt,count(distinct device_id ) as valid_show_users from valid_show group by ad_id").print();

    }
}
