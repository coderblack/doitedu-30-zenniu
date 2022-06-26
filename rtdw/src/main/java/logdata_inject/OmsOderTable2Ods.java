package logdata_inject;


import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/26
 * @Desc: 学大数据，到多易教育
 *   业务库 订单主表信息 入仓
 **/
public class OmsOderTable2Ods {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // 建flinkSql的cdc连接器表（source表），映射mysql的订单主表
        tenv.executeSql(
                "CREATE TABLE flinksql_oms_order_item_source (   \n" +
                        "  `id` bigint NOT NULL ,\n" +
                        "  `order_id` bigint  COMMENT '订单id',\n" +
                        "  `order_sn` string  COMMENT '订单编号',\n" +
                        "  `product_id` bigint ,\n" +
                        "  `product_pic` string ,\n" +
                        "  `product_name` string ,\n" +
                        "  `product_brand` string ,\n" +
                        "  `product_sn` string ,\n" +
                        "  `product_price` decimal(10,2)  COMMENT '销售价格',\n" +
                        "  `product_quantity` int  COMMENT '购买数量',\n" +
                        "  `product_sku_id` bigint  COMMENT '商品sku编号',\n" +
                        "  `product_sku_code` string  COMMENT '商品sku条码',\n" +
                        "  `product_category_id` bigint  COMMENT '商品分类id',\n" +
                        "  `sp1` string  COMMENT '商品的销售属性',\n" +
                        "  `sp2` string ,\n" +
                        "  `sp3` string ,\n" +
                        "  `promotion_name` string  COMMENT '商品促销名称',\n" +
                        "  `promotion_amount` decimal(10,2)  COMMENT '商品促销分解金额',\n" +
                        "  `coupon_amount` decimal(10,2)  COMMENT '优惠券优惠分解金额',\n" +
                        "  `integration_amount` decimal(10,2)  COMMENT '积分优惠分解金额',\n" +
                        "  `real_amount` decimal(10,2)  COMMENT '该商品经过优惠后的分解金额',\n" +
                        "  `gift_integration` int  ,\n" +
                        "  `gift_growth` int  ,\n" +
                        "  `product_attr` string  ,\n" +
                        "   PRIMARY KEY(id) NOT ENFORCED   \n" +
                        ") WITH (   \n" +
                        "     'connector' = 'mysql-cdc',   \n" +
                        "     'hostname' = 'doitedu',   \n" +
                        "     'port' = '3306',   \n" +
                        "     'username' = 'root',   \n" +
                        "     'password' = 'root',   \n" +
                        "     'database-name' = 'realtimedw',   \n" +
                        "     'table-name' = 'oms_order_item'   \n" +
                        ")"
        );

        //tenv.executeSql("select * from flinksql_oms_order_item").print();

        // 建flinkSql的 kafka连接器表（sink 表），作为订单主表数据的实时仓库ods层表
        tenv.executeSql("create table flinksql_oms_order_item_kafkasink(   \n" +
                "  `id` bigint NOT NULL ,                                         \n" +
                "  `order_id` bigint  COMMENT '订单id',                           \n" +
                "  `order_sn` string  COMMENT '订单编号',                          \n" +
                "  `product_id` bigint ,                                         \n" +
                "  `product_pic` string ,                                        \n" +
                "  `product_name` string ,                                       \n" +
                "  `product_brand` string ,                                      \n" +
                "  `product_sn` string ,                                         \n" +
                "  `product_price` decimal(10,2)  COMMENT '销售价格',              \n" +
                "  `product_quantity` int  COMMENT '购买数量',                     \n" +
                "  `product_sku_id` bigint  COMMENT '商品sku编号',                 \n" +
                "  `product_sku_code` string  COMMENT '商品sku条码',               \n" +
                "  `product_category_id` bigint  COMMENT '商品分类id',             \n" +
                "  `sp1` string  COMMENT '商品的销售属性',                          \n" +
                "  `sp2` string ,                                                \n" +
                "  `sp3` string ,                                                \n" +
                "  `promotion_name` string  COMMENT '商品促销名称',                \n" +
                "  `promotion_amount` decimal(10,2)  COMMENT '商品促销分解金额',    \n" +
                "  `coupon_amount` decimal(10,2)  COMMENT '优惠券优惠分解金额',     \n" +
                "  `integration_amount` decimal(10,2)  COMMENT '积分优惠分解金额',  \n" +
                "  `real_amount` decimal(10,2)  COMMENT '该商品经过优惠后的分解金额', \n" +
                "  `gift_integration` int  ,                          \n" +
                "  `gift_growth` int  ,                               \n" +
                "  `product_attr` string  ,                           \n" +
                "   PRIMARY KEY(id) NOT ENFORCED                      \n" +
                ") with (                                             \n" +
                " 'connector' = 'upsert-kafka',                       \n" +
                " 'topic' = 'ods-oms-order-item',                     \n" +
                " 'properties.bootstrap.servers' = 'doitedu:9092',    \n" +
                " 'key.format' = 'json',                              \n" +
                " 'value.format' = 'json'                             \n" +
                ")  ");

        //tenv.executeSql("insert into flinksql_oms_order_item_kafkasink select * from flinksql_oms_order_item_source ");



        // 建flinkSql的 hudi 连接器表（sink 表），作为订单主表数据的离线仓库ods层表
        tenv.executeSql("create table flinksql_ods_oms_order_item_sinkhudi (                                           \n" +
                "  `id` bigint   ,\n" +
                "  `order_id` bigint  ,\n" +
                "  `order_sn` string   ,\n" +
                "  `product_id` bigint ,\n" +
                "  `product_pic` string ,\n" +
                "  `product_name` string ,\n" +
                "  `product_brand` string ,\n" +
                "  `product_sn` string ,\n" +
                "  `product_price` decimal(10,2)   ,\n" +
                "  `product_quantity` int   ,\n" +
                "  `product_sku_id` bigint   ,\n" +
                "  `product_sku_code` string   ,\n" +
                "  `product_category_id` bigint   ,\n" +
                "  `sp1` string   ,\n" +
                "  `sp2` string ,\n" +
                "  `sp3` string ,\n" +
                "  `promotion_name` string   ,\n" +
                "  `promotion_amount` decimal(10,2)   ,\n" +
                "  `coupon_amount` decimal(10,2)   ,\n" +
                "  `integration_amount` decimal(10,2)   ,\n" +
                "  `real_amount` decimal(10,2)   ,\n" +
                "  `gift_integration` int  ,\n" +
                "  `gift_growth` int       ,\n" +
                "  `product_attr` string   ,\n" +
                "   PRIMARY KEY(id) NOT ENFORCED                                                \n" +
                ")                                                                              \n" +
                "with(                                                                          \n" +
                "  'connector'='hudi'                                                           \n" +
                ", 'path'= 'hdfs://doitedu:8020/hudi_lake/ods/oms_order_item'                   \n" +
                ", 'hoodie.datasource.write.recordkey.field'= 'id'                              \n" +
                ", 'write.precombine.field'= 'product_id'                                       \n" +
                ", 'write.tasks'= '1'                                                           \n" +
                ", 'compaction.tasks'= '1'                                                      \n" +
                ", 'write.rate.limit'= '2000'                                                   \n" +
                ", 'table.type'= 'COPY_ON_WRITE'                                                \n" +
                ", 'compaction.async.enabled'= 'false'                                          \n" +
                ", 'compaction.trigger.strategy'= 'num_commits'                                 \n" +
                ", 'compaction.delta_commits'= '5'                                              \n" +
                ", 'changelog.enabled'= 'true'                                                  \n" +
                ", 'read.streaming.enabled'= 'true'                                             \n" +
                ", 'read.streaming.check-interval'= '3'                                         \n" +
                ", 'hoodie.insert.shuffle.parallelism'= '2'                                     \n" +
                ", 'hive_sync.enable'= 'true'                                                   \n" +
                ", 'hive_sync.mode'= 'hms'                                                      \n" +
                ", 'hive_sync.metastore.uris'= 'thrift://doitedu:9083'                          \n" +
                ", 'hive_sync.table'= 'oms_order_item'                                          \n" +
                ", 'hive_sync.db'= 'ods'                                                        \n" +
                ", 'hive_sync.username'= ''                                                     \n" +
                ", 'hive_sync.password'= ''                                                     \n" +
                ", 'hive_sync.support_timestamp'= 'true'                                        \n" +
                ")                                                                              ");

        tenv.executeSql("insert into flinksql_ods_oms_order_item_sinkhudi select * from flinksql_oms_order_item_source ");

        env.execute();




    }
}
