package rpt_traffic;

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

        tenv.executeSql("CREATE TABLE flinksql_dwd_order_wide_source (         " +
                "  `id` bigint    ,                                                   " +
                "  `order_id` bigint  COMMENT '订单id',                               " +
                "  `order_sn` string  COMMENT '订单编号',                              " +
                "  `product_id` bigint ,                                             " +
                "  `product_pic` string ,                                            " +
                "  `product_name` string ,                                           " +
                "  `product_brand` string ,                                          " +
                "  `product_sn` string ,                                             " +
                "  `product_price` decimal(10,2)  COMMENT '销售价格',                  " +
                "  `product_quantity` int  COMMENT '购买数量',                         " +
                "  `product_sku_id` bigint  COMMENT '商品sku编号',                     " +
                "  `product_sku_code` string  COMMENT '商品sku条码',                   " +
                "  `product_category_id` bigint  COMMENT '商品分类id',                 " +
                "  `sp1` string  COMMENT '商品的销售属性',                              " +
                "  `sp2` string ,                                                    " +
                "  `sp3` string ,                                                    " +
                "  `promotion_name` string  COMMENT '商品促销名称',                    " +
                "  `product_promotion_amount` decimal(10,2)  COMMENT '商品促销分解金额',        " +
                "  `product_coupon_amount` decimal(10,2)  COMMENT '优惠券优惠分解金额',         " +
                "  `product_integration_amount` decimal(10,2)  COMMENT '积分优惠分解金额',      " +
                "  `real_amount` decimal(10,2)  COMMENT '该商品经过优惠后的分解金额',     " +
                "  `gift_integration` int  ,                              " +
                "  `gift_growth` int  ,                                   " +
                "  `product_attr` string  ,                               " +
                "  `oid` bigint NOT NULL  COMMENT '订单id',               " +
                "  `member_id` bigint NOT NULL,    " +
                "  `coupon_id` bigint ,    " +
                "  `order_sn_dup` string  COMMENT '订单编号',    " +
                "  `create_time` timestamp(3)  COMMENT '提交时间',    " +
                "  `member_username` string  COMMENT '用户帐号',    " +
                "  `total_amount` decimal(10,2)  COMMENT '订单总金额',    " +
                "  `pay_amount` decimal(10,2)  COMMENT '应付金额（实际支付金额）',    " +
                "  `freight_amount` decimal(10,2)  COMMENT '运费金额',    " +
                "  `promotion_amount` decimal(10,2)  COMMENT '促销优化金额（促销价、满减、阶梯价）',    " +
                "  `integration_amount` decimal(10,2)  COMMENT '积分抵扣金额',    " +
                "  `coupon_amount` decimal(10,2)  COMMENT '优惠券抵扣金额',    " +
                "  `discount_amount` decimal(10,2)  COMMENT '管理员后台调整订单使用的折扣金额',    " +
                "  `pay_type` int  COMMENT '支付方式：0->未支付；1->支付宝；2->微信',    " +
                "  `source_type` int  COMMENT '订单来源：0->PC订单；1->app订单',    " +
                "  `status` int  COMMENT '订单状态：0->待付款；1->待发货；2->已发货；3->已完成；4->已关闭；5->无效订单',    " +
                "  `order_type` int  COMMENT '订单类型：0->正常订单；1->秒杀订单',    " +
                "  `delivery_company` string  COMMENT '物流公司(配送方式)',    " +
                "  `delivery_sn` string  COMMENT '物流单号',    " +
                "  `auto_confirm_day` int  COMMENT '自动确认时间（天）',    " +
                "  `integration` int  COMMENT '可以获得的积分',    " +
                "  `growth` int  COMMENT '可以活动的成长值',    " +
                "  `promotion_info` string  COMMENT '活动信息',    " +
                "  `bill_type` int  COMMENT '发票类型：0->不开发票；1->电子发票；2->纸质发票',    " +
                "  `bill_header` string  COMMENT '发票抬头',    " +
                "  `bill_content` string  COMMENT '发票内容',    " +
                "  `bill_receiver_phone` string  COMMENT '收票人电话',    " +
                "  `bill_receiver_email` string  COMMENT '收票人邮箱',    " +
                "  `receiver_name` string NOT NULL COMMENT '收货人姓名',    " +
                "  `receiver_phone` string NOT NULL COMMENT '收货人电话',    " +
                "  `receiver_post_code` string  COMMENT '收货人邮编',    " +
                "  `receiver_province` string  COMMENT '省份/直辖市',    " +
                "  `receiver_city` string  COMMENT '城市',    " +
                "  `receiver_region` string  COMMENT '区',    " +
                "  `receiver_detail_address` string  COMMENT '详细地址',    " +
                "  `note` string  COMMENT '订单备注',    " +
                "  `confirm_status` int  COMMENT '确认收货状态：0->未确认；1->已确认',    " +
                "  `delete_status` int   COMMENT '删除状态：0->未删除；1->已删除',    " +
                "  `use_integration` int  COMMENT '下单时使用的积分',    " +
                "  `payment_time` timestamp(3)  COMMENT '支付时间',    " +
                "  `delivery_time` timestamp(3)  COMMENT '发货时间',    " +
                "  `receive_time` timestamp(3)  COMMENT '确认收货时间',    " +
                "  `comment_time` timestamp(3)  COMMENT '评价时间',    " +
                "  `modify_time` timestamp(3)  COMMENT '修改时间',    " +
                "   watermark for create_time as create_time - interval '0' second " +
                "   -- primary key (id) not enforced                    " +
                ") with (                                                 " +
                " 'connector' = 'kafka',                              " +
                " 'topic' = 'dwd-order-wide',                            " +
                " 'properties.group.id' = 'gp01',                            " +
                " 'scan.startup.mode' = 'earliest-offset',                            " +
                " 'properties.bootstrap.servers' = 'doitedu:9092',        " +
                " -- 'key.format' = 'json',                                  " +
                " 'value.format' = 'json'                                 " +
                ")  ");


        tenv.executeSql("SELECT    " +
                "  window_start,    " +
                "  window_end,    " +
                "  product_category_id,    " +
                "  sum(product_price * product_quantity ) as order_amt,    " +
                "  sum(real_amount) as order_pay_amt,    " +
                "  sum(promotion_amount + coupon_amount + integration_amount) as discount_amt    " +
                "FROM  TABLE(       " +
                " CUMULATE( TABLE flinksql_dwd_order_wide_source, descriptor(create_time), interval '5'  minute , interval '24' hour)    " +
                ")    " +
                "group by window_start,window_end,product_category_id    ").print();


    }
}
