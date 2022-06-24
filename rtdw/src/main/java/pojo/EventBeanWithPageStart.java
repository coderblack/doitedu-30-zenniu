package pojo;

/**
 * Copyright 2022 bejson.com
 */
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.table.annotation.DataTypeHint;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class EventBeanWithPageStart {

    public String account;
    public String appid;
    public String appversion;
    public String carrier;
    public String deviceid;
    public String devicetype;
    public String eventid;
    public String ip;
    public Double latitude;
    public Double longitude;
    public String nettype;
    public String osname;
    public String osversion;
    public Map<String,String> properties;
    public String releasechannel;
    public String resolution;
    public String sessionid;
    public long timestamp;
    public long guid;
    // 如果是注册用户，则这里表示注册的时间
    public long registerTime;
    // 如果是非注册用户，则这里表示首次到访时间
    public long firstAccessTime;
    // 新老访客属性
    public int isNew;
    // geohash码
    public String geoHashCode;
    // 省市区维度字段
    public String province;
    public String city;
    public String region;

    public long pageStartTime;


}