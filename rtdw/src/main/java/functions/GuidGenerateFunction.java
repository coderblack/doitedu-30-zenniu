package functions;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hudi.org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import pojo.DeviceAccount;
import pojo.EventBean;

import java.sql.*;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;

public class GuidGenerateFunction extends KeyedProcessFunction<String, EventBean, EventBean> {

    PreparedStatement preparedStatement;

    MapState<String, Tuple2<Integer, Long>> accountIdState;

    MapState<String, String> accountSessionIdState;

    org.apache.hadoop.hbase.client.Connection hbaseConn;

    Table deviceAccountBindTable;

    Table deviceTempGuidTable;

    Table guidGeneratorTable;

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk", "root", "root");
        preparedStatement = conn.prepareStatement("select id,register_time from ums_member where account = ?");


        // 用于缓存  “账号”->id
        accountIdState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("account_id", TypeInformation.of(String.class)
                        , TypeInformation.of(new TypeHint<Tuple2<Integer, Long>>() {
                })));


        // 用于记录出现过的  账号-会话id
        MapStateDescriptor<String, String> accountSessionIdStateDesc = new MapStateDescriptor<String, String>("account_sessionid", String.class, String.class);
        accountSessionIdStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.hours(2)).build());
        accountSessionIdState = getRuntimeContext().getMapState(accountSessionIdStateDesc);

        // 创建hbase连接
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "doitedu:2181");
        hbaseConn = ConnectionFactory.createConnection(conf);

        deviceAccountBindTable = hbaseConn.getTable(TableName.valueOf("device_account_bind"));

        deviceTempGuidTable = hbaseConn.getTable(TableName.valueOf("device_temp_guid"));

        guidGeneratorTable = hbaseConn.getTable(TableName.valueOf("guid_counter"));

    }

    @Override
    public void processElement(EventBean eventBean, KeyedProcessFunction<String, EventBean, EventBean>.Context ctx, Collector<EventBean> out) throws Exception {

        if (StringUtils.isNotBlank(eventBean.getAccount())) {
            haveAccountProcess(eventBean, true);
        }

        // 如果数据中没有账号，则去hbase的设备账号绑定表中求权重最大的账号
        if (StringUtils.isBlank(eventBean.getAccount())) {
            Result result = deviceAccountBindTable.get(new Get(Bytes.toBytes(eventBean.getDeviceid())));

            if (result.getExists()) {
                // 如果权重表中存在此设备
                byte[] jsonBytes = result.getValue("f".getBytes(), "q".getBytes());
                List<DeviceAccount> deviceAccounts = JSON.parseArray(Bytes.toString(jsonBytes), DeviceAccount.class);
                Collections.sort(deviceAccounts);
                String bindAccount = deviceAccounts.get(0).getAccount();
                // 按有账号，但不用处理权重的方法计算
                haveAccountProcess(eventBean, false);
            } else {
                // 如果权重表中不存在此设备，则要去 “设备-临时guid” 映射表找id
                Result res = deviceTempGuidTable.get(new Get(Bytes.toBytes(eventBean.getDeviceid())));
                if (res.getExists()) {
                    // 存在
                    byte[] tempGuidBytes = res.getValue("f".getBytes(), "guid".getBytes());
                    long tempGuid = Bytes.toLong(tempGuidBytes);

                    byte[] firstAccTimeBytes = res.getValue("f".getBytes(), "firstAccTime".getBytes());
                    long firstAccTime = Bytes.toLong(firstAccTimeBytes);

                    eventBean.setGuid(tempGuid);
                    eventBean.setFirstAccessTime(firstAccTime);

                } else {
                    // 不存在，则去请求计数器获取一个新的id
                    long newGuid = guidGeneratorTable.incrementColumnValue("r".getBytes(), "f".getBytes(), "q".getBytes(), 1L);
                    eventBean.setGuid(newGuid);
                    eventBean.setFirstAccessTime(eventBean.getTimestamp());

                    // 将  设备-临时guid，  插入  “设备-临时guid映射表”
                    Put put = new Put(Bytes.toBytes(eventBean.getDeviceid()));
                    put.addColumn("f".getBytes(), "guid".getBytes(), Bytes.toBytes(newGuid));
                    put.addColumn("f".getBytes(), "firstAccTime".getBytes(), Bytes.toBytes(eventBean.getTimestamp()));
                    deviceTempGuidTable.put(put);

                }
            }
        }


        // 输出结果
        out.collect(eventBean);
    }

    private void haveAccountProcess(EventBean eventBean, boolean needProcessWeight) throws Exception {
        // 如果数据中有账号,则去mysql查询该账号的id
        // 先从缓存中，查找该账号对应的id及注册时间
        Tuple2<Integer, Long> idAndRegisterTime = accountIdState.get(eventBean.getAccount());
        if (idAndRegisterTime != null) {
            eventBean.setGuid(idAndRegisterTime.f0);
            eventBean.setRegisterTime(idAndRegisterTime.f1);
        } else {
            preparedStatement.setString(1, eventBean.getAccount());
            ResultSet resultSet = preparedStatement.executeQuery();

            Integer id = null;
            Timestamp timestamp = null;

            while (resultSet.next()) {
                id = resultSet.getInt(1);
                timestamp = resultSet.getTimestamp(2);
            }

            // 如果在mysql中查询到了账号id，则放入缓存
            if (id != null) {
                accountIdState.put(eventBean.getAccount(), Tuple2.of(id, timestamp.getTime()));

                // 填充guid到eventBean
                eventBean.setGuid(id);
                eventBean.setRegisterTime(timestamp.getTime());

                // 将权重绑定表中，该设备对应的该账号的绑定权重提分，对其他的则要减分
                if (!accountSessionIdState.contains(eventBean.getAccount() + eventBean.getSessionid()) && needProcessWeight) {
                    // 则要去hbase中进行增减分了
                    Result result = deviceAccountBindTable.get(new Get(Bytes.toBytes(eventBean.getDeviceid())));
                    if (!result.isEmpty()) {
                        byte[] deviceAccountBindJsonBytes = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("q"));
                        String json = Bytes.toString(deviceAccountBindJsonBytes);
                        List<DeviceAccount> deviceAccounts = JSON.parseArray(json, DeviceAccount.class);

                        boolean flag = false;
                        for (DeviceAccount deviceAccount : deviceAccounts) {
                            if (deviceAccount.getAccount().equals(eventBean.getAccount())) {
                                deviceAccount.setScore(deviceAccount.getScore() + 1);
                                flag = true;
                            } else {
                                deviceAccount.setScore(deviceAccount.getScore() * 0.7);
                            }
                        }
                        if (!flag) {
                            DeviceAccount deviceAccount = new DeviceAccount(eventBean.getAccount(), 1.0, eventBean.getTimestamp());
                            deviceAccounts.add(deviceAccount);
                        }

                        // 是把处理好的分数，重新更新回hbase中去
                        Put put = new Put(Bytes.toBytes(eventBean.getDeviceid()));
                        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes(JSON.toJSONString(deviceAccounts)));
                        deviceAccountBindTable.put(put);
                    } else {

                        List<DeviceAccount> lst = Collections.singletonList(new DeviceAccount(eventBean.getAccount(), 1.0, eventBean.getTimestamp()));
                        Put put = new Put(Bytes.toBytes(eventBean.getDeviceid()));
                        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes(JSON.toJSONString(lst)));
                        deviceAccountBindTable.put(put);
                    }
                }

                // 往会话id状态中存入一条状态
                accountSessionIdState.put(eventBean.getAccount() + eventBean.getSessionid(), "");
            }
        }
    }
}