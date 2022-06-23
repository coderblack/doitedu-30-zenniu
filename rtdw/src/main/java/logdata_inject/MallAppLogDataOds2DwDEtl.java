package logdata_inject;

import com.alibaba.fastjson.JSON;
import functions.Geohash2AreaFunction;
import functions.Gps2GeohashFunction;
import functions.GuidGenerateFunction;
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
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
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

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/23
 * @Desc: 学大数据，到多易教育
 * 商城app通用行为日志 ods -> dwd 处理任务
 * 数据源： kafka 的ods层
 * 落地： kafka的明细层
 * 处理需求：  清洗、过滤、全局guid生成、地理位置维度信息打宽
 * <p>
 * <p>
 * {"account":"uupumm","appId":"cn.doitedu.yinew","appVersion":"2.6","carrier":"华为移动","deviceId":"gpgy-1061","deviceType":"iphone6","eventId":"e_gp_3","ip":"10.21.101.65","latitude":15.70554442983904,"longitude":129.69086590029062,"netType":"WIFI","osName":"ios","osVersion":"8.0","properties":{"p12":"v2","p14":"v5","p17":"v2","pageId":"index"},"releaseChannel":"apple-store","resolution":"2048*1366","sessionId":"GXNIUEBUWO","testGuid":163,"timeStamp":1655955439174}
 * <p>
 * 前期准备，hbase中要建表
 * 设备-账号绑定表：    create 'device_account_bind','f'
 * 数据结构方式1 ：  deviceId => f:account01->80,f:account03->60
 * 数据结构方式2 ：  deviceId => f:q->"[{account:a,score:80},{account:a,score:80}]"
 * <p>
 * 设备临时GUID映射表： create 'device_temp_guid','f'
 * 数据结构：  deviceId => f:q->100000001
 * id生成器表（计数器）:  create 'guid_counter','f'
 * 数据结构：  'rk' => f:q->100000000
 **/
public class MallAppLogDataOds2DwDEtl {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从kafka的 商城app通用行为日志topic中消费埋点日志数据
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setGroupId("doe-001")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("mall-app-log")
                .build();


        DataStream<String> logStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "mall-app-source", TypeInformation.of(String.class));

        // 把json数据变成pojo数据
        DataStream<EventBean> beanStream = logStream.map(value -> JSON.parseObject(value, EventBean.class));

        // keyBy（deviceId) 这里，一方面是便于设置下游算子的并行度，另一方面，可以避免“相同设备号数据”进入多个subtask而产生guid计数器冲突
        SingleOutputStreamOperator<EventBean> resultStream = beanStream.keyBy(EventBean::getDeviceid)
                .process(new GuidGenerateFunction())  // guid生成
                .process(new Gps2GeohashFunction())
                .keyBy(EventBean::getGeoHashCode)
                .process(new Geohash2AreaFunction());

        // 获取到无法解析的gps坐标数据侧流
        DataStream<String> unknownGps = resultStream.getSideOutput(new OutputTag<>("unknown_gps", TypeInformation.of(String.class)));

        // TODO  将未能解析的gps输出到一个存储中（如kafka），然后交给另外一个flink job去消费及请求高德服务来填充我们的地理位置维表
        unknownGps.print("unknown_gps");

        // 将处理好的dwd数据落地到kafka的dwd层topic
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic("dwd-mall-app-log")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setTransactionalIdPrefix("doitedu-")
                .build();
        resultStream.map(JSON::toJSONString).sinkTo(kafkaSink);


        env.execute();
    }
}
