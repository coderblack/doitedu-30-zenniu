package cn.doitedu.rtmk.validate;

import cn.doitedu.rtmk.validate.utils.SqlHolder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/19
 * @Desc: 学大数据，到多易教育
 *    商城用户行为事件从kafka接入doris
 **/
public class MallEventsToDoirs {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // kafka  source 表定义
        tenv.executeSql(SqlHolder.KAFKA_EVENTS_SOURCE_DDL);

        // doris sink表定义
        tenv.executeSql(SqlHolder.DORIS_EVENTS_DETAIL_SINK_DDL);

        // 执行insert语句
        tenv.executeSql(SqlHolder.DORIS_EVENTS_DETAIL_SINK_DML).print();


    }
}
