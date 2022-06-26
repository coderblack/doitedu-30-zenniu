package traffic_rpt;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Order_RPT_01 {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        tenv.executeSql("CREATE TABLE flinksql_dwd_order_wide_source (    \n " +
                "  `id` bigint    ,                                               \n" +
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
                "  `product_promotion_amount` decimal(10,2)  COMMENT '商品促销分解金额',    \n" +
                "  `product_coupon_amount` decimal(10,2)  COMMENT '优惠券优惠分解金额',     \n" +
                "  `product_integration_amount` decimal(10,2)  COMMENT '积分优惠分解金额',  \n" +
                "  `real_amount` decimal(10,2)  COMMENT '该商品经过优惠后的分解金额', \n" +
                "  `gift_integration` int  ,                          \n" +
                "  `gift_growth` int  ,                               \n" +
                "  `product_attr` string  ,                           \n" +
                "  `oid` bigint NOT NULL  COMMENT '订单id',           \n" +
                "  `member_id` bigint NOT NULL,\n" +
                "  `coupon_id` bigint ,\n" +
                "  `order_sn_dup` string  COMMENT '订单编号',\n" +
                "  `create_time` timestamp(3)  COMMENT '提交时间',\n" +
                "  `member_username` string  COMMENT '用户帐号',\n" +
                "  `total_amount` decimal(10,2)  COMMENT '订单总金额',\n" +
                "  `pay_amount` decimal(10,2)  COMMENT '应付金额（实际支付金额）',\n" +
                "  `freight_amount` decimal(10,2)  COMMENT '运费金额',\n" +
                "  `promotion_amount` decimal(10,2)  COMMENT '促销优化金额（促销价、满减、阶梯价）',\n" +
                "  `integration_amount` decimal(10,2)  COMMENT '积分抵扣金额',\n" +
                "  `coupon_amount` decimal(10,2)  COMMENT '优惠券抵扣金额',\n" +
                "  `discount_amount` decimal(10,2)  COMMENT '管理员后台调整订单使用的折扣金额',\n" +
                "  `pay_type` int  COMMENT '支付方式：0->未支付；1->支付宝；2->微信',\n" +
                "  `source_type` int  COMMENT '订单来源：0->PC订单；1->app订单',\n" +
                "  `status` int  COMMENT '订单状态：0->待付款；1->待发货；2->已发货；3->已完成；4->已关闭；5->无效订单',\n" +
                "  `order_type` int  COMMENT '订单类型：0->正常订单；1->秒杀订单',\n" +
                "  `delivery_company` string  COMMENT '物流公司(配送方式)',\n" +
                "  `delivery_sn` string  COMMENT '物流单号',\n" +
                "  `auto_confirm_day` int  COMMENT '自动确认时间（天）',\n" +
                "  `integration` int  COMMENT '可以获得的积分',\n" +
                "  `growth` int  COMMENT '可以活动的成长值',\n" +
                "  `promotion_info` string  COMMENT '活动信息',\n" +
                "  `bill_type` int  COMMENT '发票类型：0->不开发票；1->电子发票；2->纸质发票',\n" +
                "  `bill_header` string  COMMENT '发票抬头',\n" +
                "  `bill_content` string  COMMENT '发票内容',\n" +
                "  `bill_receiver_phone` string  COMMENT '收票人电话',\n" +
                "  `bill_receiver_email` string  COMMENT '收票人邮箱',\n" +
                "  `receiver_name` string NOT NULL COMMENT '收货人姓名',\n" +
                "  `receiver_phone` string NOT NULL COMMENT '收货人电话',\n" +
                "  `receiver_post_code` string  COMMENT '收货人邮编',\n" +
                "  `receiver_province` string  COMMENT '省份/直辖市',\n" +
                "  `receiver_city` string  COMMENT '城市',\n" +
                "  `receiver_region` string  COMMENT '区',\n" +
                "  `receiver_detail_address` string  COMMENT '详细地址',\n" +
                "  `note` string  COMMENT '订单备注',\n" +
                "  `confirm_status` int  COMMENT '确认收货状态：0->未确认；1->已确认',\n" +
                "  `delete_status` int   COMMENT '删除状态：0->未删除；1->已删除',\n" +
                "  `use_integration` int  COMMENT '下单时使用的积分',\n" +
                "  `payment_time` timestamp(3)  COMMENT '支付时间',\n" +
                "  `delivery_time` timestamp(3)  COMMENT '发货时间',\n" +
                "  `receive_time` timestamp(3)  COMMENT '确认收货时间',\n" +
                "  `comment_time` timestamp(3)  COMMENT '评价时间',\n" +
                "  `modify_time` timestamp(3)  COMMENT '修改时间',\n" +
                "   watermark for create_time as create_time - interval '0' second " +
                "   -- primary key (id) not enforced                \n" +
                ") with (                                             \n" +
                " 'connector' = 'kafka',                          \n" +
                " 'topic' = 'dwd-order-wide',                        \n" +
                " 'properties.group.id' = 'gp01',                        \n" +
                " 'scan.startup.mode' = 'earliest-offset',                        \n" +
                " 'properties.bootstrap.servers' = 'doitedu:9092',    \n" +
                " -- 'key.format' = 'json',                              \n" +
                " 'value.format' = 'json'                             \n" +
                ")  ");


        tenv.executeSql("SELECT\n" +
                "  window_start,\n" +
                "  window_end,\n" +
                "  product_category_id,\n" +
                "  sum(product_price * product_quantity ) as order_amt,\n" +
                "  sum(real_amount) as order_pay_amt,\n" +
                "  sum(promotion_amount + coupon_amount + integration_amount) as discount_amt\n" +
                "\n" +
                "FROM  TABLE(   \n" +
                " CUMULATE( TABLE flinksql_dwd_order_wide_source, descriptor(create_time), interval '5'  minute , interval '24' hour)\n" +
                ")\n" +
                "group by window_start,window_end,product_category_id\n").print();


    }
}
