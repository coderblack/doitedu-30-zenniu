package pojo;

/**
 * Copyright 2022 bejson.com
 */
import lombok.*;
import org.apache.flink.table.annotation.DataTypeHint;

import java.util.Map;


public class EventBean {

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
    public @DataTypeHint("MAP<STRING,STRING>") Map<String,String> properties;
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

    // 新老会员属性
    //private int isNewMember;

    // geohash码
    public String geoHashCode;

    // 省市区维度字段
    public String province;
    public String city;
    public String region;

    // properties的json格式字段
    public String propsJson;


    public EventBean() {
    }

    public EventBean(String account, String appid, String appversion, String carrier, String deviceid, String devicetype, String eventid, String ip, Double latitude, Double longitude, String nettype, String osname, String osversion, Map<String, String> properties, String releasechannel, String resolution, String sessionid, long timestamp, long guid, long registerTime, long firstAccessTime, int isNew, String geoHashCode, String province, String city, String region, String propsJson) {
        this.account = account;
        this.appid = appid;
        this.appversion = appversion;
        this.carrier = carrier;
        this.deviceid = deviceid;
        this.devicetype = devicetype;
        this.eventid = eventid;
        this.ip = ip;
        this.latitude = latitude;
        this.longitude = longitude;
        this.nettype = nettype;
        this.osname = osname;
        this.osversion = osversion;
        this.properties = properties;
        this.releasechannel = releasechannel;
        this.resolution = resolution;
        this.sessionid = sessionid;
        this.timestamp = timestamp;
        this.guid = guid;
        this.registerTime = registerTime;
        this.firstAccessTime = firstAccessTime;
        this.isNew = isNew;
        this.geoHashCode = geoHashCode;
        this.province = province;
        this.city = city;
        this.region = region;
        this.propsJson = propsJson;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getAppid() {
        return appid;
    }

    public void setAppid(String appid) {
        this.appid = appid;
    }

    public String getAppversion() {
        return appversion;
    }

    public void setAppversion(String appversion) {
        this.appversion = appversion;
    }

    public String getCarrier() {
        return carrier;
    }

    public void setCarrier(String carrier) {
        this.carrier = carrier;
    }

    public String getDeviceid() {
        return deviceid;
    }

    public void setDeviceid(String deviceid) {
        this.deviceid = deviceid;
    }

    public String getDevicetype() {
        return devicetype;
    }

    public void setDevicetype(String devicetype) {
        this.devicetype = devicetype;
    }

    public String getEventid() {
        return eventid;
    }

    public void setEventid(String eventid) {
        this.eventid = eventid;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public String getNettype() {
        return nettype;
    }

    public void setNettype(String nettype) {
        this.nettype = nettype;
    }

    public String getOsname() {
        return osname;
    }

    public void setOsname(String osname) {
        this.osname = osname;
    }

    public String getOsversion() {
        return osversion;
    }

    public void setOsversion(String osversion) {
        this.osversion = osversion;
    }

    @DataTypeHint("MAP<STRING,STRING>")
    public Map<String, String> getProperties() {
        return properties;
    }

    @DataTypeHint("MAP<STRING,STRING>")
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getReleasechannel() {
        return releasechannel;
    }

    public void setReleasechannel(String releasechannel) {
        this.releasechannel = releasechannel;
    }

    public String getResolution() {
        return resolution;
    }

    public void setResolution(String resolution) {
        this.resolution = resolution;
    }

    public String getSessionid() {
        return sessionid;
    }

    public void setSessionid(String sessionid) {
        this.sessionid = sessionid;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getGuid() {
        return guid;
    }

    public void setGuid(long guid) {
        this.guid = guid;
    }

    public long getRegisterTime() {
        return registerTime;
    }

    public void setRegisterTime(long registerTime) {
        this.registerTime = registerTime;
    }

    public long getFirstAccessTime() {
        return firstAccessTime;
    }

    public void setFirstAccessTime(long firstAccessTime) {
        this.firstAccessTime = firstAccessTime;
    }

    public int getIsNew() {
        return isNew;
    }

    public void setIsNew(int isNew) {
        this.isNew = isNew;
    }

    public String getGeoHashCode() {
        return geoHashCode;
    }

    public void setGeoHashCode(String geoHashCode) {
        this.geoHashCode = geoHashCode;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getPropsJson() {
        return propsJson;
    }

    public void setPropsJson(String propsJson) {
        this.propsJson = propsJson;
    }
}