package cn.doitedu.rtmk.validate;

import cn.doitedu.rtmk.validate.pojo.EventUnitCondition;
import cn.doitedu.rtmk.validate.pojo.MarketingRule;
import cn.doitedu.rtmk.validate.pojo.RuleManagementPojo;
import cn.doitedu.rtmk.validate.pojo.UserMallEvent;
import cn.doitedu.rtmk.validate.utils.EventUtil;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Map;

public class RuleEngineValidate {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");


        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics("zen-mall-events")
                .setGroupId("gp04")
                .setBootstrapServers("doitedu:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("auto.offset.commit", "false")
                .build();

        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk-source");//  接收的是 Source 接口的实现类
        DataStream<UserMallEvent> eventStream = streamSource.map(s -> JSON.parseObject(s, UserMallEvent.class));

        // 用flinksql的cdc连接器，去连接 规则元数据库，实时获取规则管理信息（新增规则，删除规则，修改更新规则，下线规则，上线规则）
        /*
        CREATE TABLE `rule_mgmt` (
         `id` int(11) NOT NULL AUTO_INCREMENT,
         `rule_name` varchar(255) DEFAULT NULL,
         `rule_condition_json` varchar(10240) DEFAULT NULL,
         `rule_controller_drl` varchar(40960) DEFAULT NULL,
         `rule_status` varchar(255) DEFAULT NULL,
         `create_time` datetime DEFAULT NULL,
         `modify_time` datetime DEFAULT NULL,
         `publisher` varchar(255) DEFAULT NULL,
         PRIMARY KEY (`id`)
       ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
         */
        tenv.executeSql("CREATE TABLE rule_mgmt_flink ( " +
                "      id INT,                      " +
                "      rule_name string,            " +
                "      rule_condition_json string,  " +
                "      rule_controller_drl string,  " +
                "      rule_status string,          " +
                "      create_time string,          " +
                "      modify_time string,          " +
                "      publisher string  ,          " +
                "     PRIMARY KEY(id) NOT ENFORCED  " +
                "     ) WITH (                      " +
                "     'connector' = 'mysql-cdc',    " +
                "     'hostname' = 'doitedu',       " +
                "     'port' = '3306',              " +
                "     'username' = 'root',          " +
                "     'password' = 'root',          " +
                "     'database-name' = 'rtmk',     " +
                "     'table-name' = 'rule_mgmt'    " +
                ")");

        // 将有名表，变成table对象
        Table table = tenv.from("rule_mgmt_flink");
        DataStream<Row> ruleInfoChangelogStream = tenv.toChangelogStream(table);
        // 对changelog流进行筛选，只留下 +I 和 +U的变化记录
        SingleOutputStreamOperator<Row> filtered = ruleInfoChangelogStream.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row row) throws Exception {
                return row.getKind().toByteValue() != 1 && row.getKind().toByteValue() != 3;
            }
        });

        // 把row数据变成内部的pojo数据
        SingleOutputStreamOperator<RuleManagementPojo> ruleManagementStream = filtered.map(new MapFunction<Row, RuleManagementPojo>() {
            @Override
            public RuleManagementPojo map(Row row) throws Exception {

                int id = row.<Integer>getFieldAs("id");
                String rule_name = row.<String>getFieldAs("rule_name");

                String rule_condition_json = row.<String>getFieldAs("rule_condition_json");
                // 将规则参数json转成MarketingRule对象
                MarketingRule marketingRule = JSON.parseObject(rule_condition_json, MarketingRule.class);

                String rule_controller_drl = row.<String>getFieldAs("rule_controller_drl");
                String rule_status = row.<String>getFieldAs("rule_status");
                String create_time = row.<String>getFieldAs("create_time");
                String modify_time = row.<String>getFieldAs("modify_time");
                String publisher = row.<String>getFieldAs("publisher");

                byte kindByteValue = row.getKind().toByteValue();
                int operateType = kindByteValue == 0 ? 1 : 2;

                return new RuleManagementPojo(operateType, id, rule_name, marketingRule, rule_controller_drl, rule_status, create_time, modify_time, publisher);
            }
        });
        /*ruleManagementStream.print();*/


        // 广播规则信息，并连接到  事件流
        MapStateDescriptor<Integer, RuleManagementPojo> broadCastStateDesc = new MapStateDescriptor<>("ruleMgmtPojo", Integer.class, RuleManagementPojo.class);
        BroadcastStream<RuleManagementPojo> ruleBroadCastStream = ruleManagementStream.broadcast(broadCastStateDesc);

        eventStream
                .keyBy(UserMallEvent::getTestGuid)
                .connect(ruleBroadCastStream)
                .process(new KeyedBroadcastProcessFunction<Long, UserMallEvent, RuleManagementPojo, String>() {
                    @Override
                    public void processElement(UserMallEvent event, KeyedBroadcastProcessFunction<Long, UserMallEvent, RuleManagementPojo, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {

                        // 规则信息存储状态中，可能有很多的规则
                        ReadOnlyBroadcastState<Integer, RuleManagementPojo> ruleState = ctx.getBroadcastState(broadCastStateDesc);

                        for (Map.Entry<Integer, RuleManagementPojo> ruleEntry : ruleState.immutableEntries()) {

                            if(  !ruleEntry.getValue().getRule_status().equals("1")) break;

                            EventUnitCondition triggerEventCondition = ruleEntry.getValue().getMarketingRule().getTriggerEventCondition();

                            // 用户的此次行为，是否满足这条规则的触发条件  ( 事件id相同，事件属性满足）
                            boolean isTrig = EventUtil.eventMatchCondition(event, triggerEventCondition);

                            // 如果触发，则去计算规则中的各种属性条件是否满足

                            // 如果规则完全满足，则输出触达信息

                            if(isTrig) {
                                out.collect(String.format("%d 用户， %s 事件 ，触发了规则： %d ",event.getTestGuid(),event.getEventId(),ruleEntry.getKey()));
                            }
                        }
                    }

                    @Override
                    public void processBroadcastElement(RuleManagementPojo ruleManagementPojo, KeyedBroadcastProcessFunction<Long, UserMallEvent, RuleManagementPojo, String>.Context ctx, Collector<String> out) throws Exception {
                        System.out.println("注入一条规则,规则id为： " + ruleManagementPojo.getId());

                        // 规则动态发布功能在flink内部的注入处理
                        // 规则动态发布平台所做的：  新增规则，修改规则，下线规则，上线规则，停用规则…… 在我们的规则注入模块内部，都是一个put操作
                        BroadcastState<Integer, RuleManagementPojo> ruleState = ctx.getBroadcastState(broadCastStateDesc);
                        ruleState.put(ruleManagementPojo.getId(), ruleManagementPojo);
                    }
                }).print();


        env.execute();

    }
}
