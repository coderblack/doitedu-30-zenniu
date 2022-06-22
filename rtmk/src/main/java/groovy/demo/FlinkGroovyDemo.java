package groovy.demo;


import groovy.lang.GroovyClassLoader;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Map;

public class FlinkGroovyDemo {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // 从数据库中抓取规则信息
        tenv.executeSql("CREATE TABLE rule_mgmt ( " +
                "      ruleId INT,                          " +
                "      rule_match_state_impl  string ,      " +
                "     PRIMARY KEY(ruleId) NOT ENFORCED      " +
                "     ) WITH (                              " +
                "     'connector' = 'mysql-cdc',            " +
                "     'hostname' = 'doitedu',               " +
                "     'port' = '3306',                      " +
                "     'username' = 'root',                  " +
                "     'password' = 'root',                  " +
                "     'database-name' = 'rtmk',             " +
                "     'table-name' = 'rule_info'            " +
                ")");
        Table table = tenv.from("rule_mgmt");
        DataStream<Row> ruleStream = tenv.toChangelogStream(table);

        // u01,e01,10
        DataStreamSource<String> eventStream = env.socketTextStream("doitedu", 9999);
        DataStream<EventBean> eventBeanStream = eventStream.map(s -> {
            String[] split = s.split(",");
            return new EventBean(split[0], split[1], Integer.parseInt(split[2]));
        });

        // 广播 规则流
        MapStateDescriptor<Integer, String> desc = new MapStateDescriptor<>("ruleState", Integer.class, String.class);
        BroadcastStream<Row> broadcastStream = ruleStream.broadcast(desc);

        // 连接 事件流 和 规则流
        BroadcastConnectedStream<EventBean, Row> connectedStream = eventBeanStream
                .keyBy(bean->bean.getGuid())
                .connect(broadcastStream);

        // 处理逻辑
        connectedStream.process(new KeyedBroadcastProcessFunction<String, EventBean, Row, String>() {
            ListState<EventBean> eventState;
            @Override
            public void open(Configuration parameters) throws Exception {

                eventState = getRuntimeContext().getListState(new ListStateDescriptor<EventBean>("eventState", EventBean.class));
            }

            @Override
            public void processElement(EventBean eventBean, KeyedBroadcastProcessFunction<String, EventBean, Row, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {

                // 将事件存入状态
                eventState.add(eventBean);

                ReadOnlyBroadcastState<Integer, String> ruleMapState = ctx.getBroadcastState(desc);

                for (Map.Entry<Integer, String> immutableEntry : ruleMapState.immutableEntries()) {
                    //System.out.println("发现了至少一条规则，准备执行匹配计算......");
                    String ruleMatchCode = immutableEntry.getValue();

                    // drools
                    GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
                    Class aClass = groovyClassLoader.parseClass(ruleMatchCode);

                    RuleStateMatcher ruleStateMatcher = (RuleStateMatcher) aClass.newInstance();

                    // 用本规则自带的匹配逻辑去匹配规则
                    boolean isMatch = ruleStateMatcher.matchRuleInState(eventState);
                    //System.out.println("本次计算的结果是：" + isMatch);


                    if(isMatch)  out.collect(String.format("用户： %s , 在时刻： %d , 匹配命中了规则： %d ",eventBean.getGuid(),System.currentTimeMillis(),immutableEntry.getKey()));

                }

            }

            @Override
            public void processBroadcastElement(Row ruleInfoRow, KeyedBroadcastProcessFunction<String, EventBean, Row, String>.Context ctx, Collector<String> out) throws Exception {

                BroadcastState<Integer, String> ruleMapState = ctx.getBroadcastState(desc);
                int ruleId = ruleInfoRow.getFieldAs(0);
                String ruleMatchCode = ruleInfoRow.getFieldAs(1);

                System.out.printf("注入了一条规则： %d , 规则计算代码： %s" ,ruleId,ruleMatchCode);

                ruleMapState.put(ruleId,ruleMatchCode);

            }
        }).print();


        env.execute();


    }

}
