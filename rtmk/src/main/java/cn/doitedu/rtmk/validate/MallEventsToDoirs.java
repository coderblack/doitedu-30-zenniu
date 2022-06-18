package cn.doitedu.rtmk.validate;

import cn.doitedu.rtmk.validate.utils.SqlHolder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652  @WX: doitedu2018
 * @Date: 2022/6/18
 * @Desc: 商城用户行为事件从kafka接入doris
 **/
public class MallEventsToDoirs {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        tenv.executeSql(SqlHolder.KAFKA_EVENTS_SOURCE_DDL);
        tenv.executeSql(SqlHolder.DORIS_DETAIL_SINK_DDL);
        tenv.executeSql(SqlHolder.DORIS_DETAIL_SINK_DML).print();


    }
}
