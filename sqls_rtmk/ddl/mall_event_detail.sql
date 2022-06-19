-- doris ，商城用户事件明细数据建表
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
    PARTITION `p2020531` VALUES LESS THAN ("2022-06-01"),
    PARTITION `p2020601` VALUES LESS THAN ("2022-06-02"),
    PARTITION `p2020602` VALUES LESS THAN ("2022-06-03"),
    PARTITION `p2020603` VALUES LESS THAN ("2022-06-04")
)
DISTRIBUTED BY HASH(`guid`) BUCKETS 1
PROPERTIES
(
  "replication_num" = "1"
);

