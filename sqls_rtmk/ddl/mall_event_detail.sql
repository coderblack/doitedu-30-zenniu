-- 在 doris 中建表，商城用户事件明细数据建表
CREATE TABLE dwd_mall_app_events
(
    guid                   BIGINT ,
    eventid                String ,
    eventTime              BIGINT ,
    dw_date                DATE   ,
    releasechannel         String ,
    account                String ,
    appid                  String ,
    appversion             String ,
    carrier                String ,
    deviceid               String ,
    devicetype             String ,
    ip                     String ,
    latitude               double ,
    longitude              double ,
    nettype                String ,
    osname                 String ,
    osversion              String ,
    resolution             String ,
    sessionid              String ,
    propsJson              String

)
UNIQUE KEY(`guid`, `eventid`,`eventTime`,`dw_date`)
PARTITION BY RANGE(`dw_date`)
(
    PARTITION `p20220619` VALUES LESS THAN ("2022-06-20"),
    PARTITION `p20220620` VALUES LESS THAN ("2022-06-21"),
    PARTITION `p20220621` VALUES LESS THAN ("2022-06-22"),
    PARTITION `p20220622` VALUES LESS THAN ("2022-06-23")
)
DISTRIBUTED BY HASH(`guid`) BUCKETS 1
PROPERTIES
(
  "replication_num" = "1"
);

