package cn.doitedu.rtmk.validate.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserMallEvent {

    private long guid;
    private String account        ;
    private String appId          ;
    private String appVersion     ;
    private String carrier        ;
    private String deviceId       ;
    private String deviceType     ;
    private String ip             ;
    private double latitude       ;
    private double longitude      ;
    private String netType        ;
    private String osName         ;
    private String osVersion      ;
    private String releaseChannel ;
    private String resolution     ;
    private String sessionId      ;
    private long timeStamp        ;
    private String eventId        ;
    private Map<String,String> properties;

    // 在添加动态keyby后添加的一个功能字段
    private String keyByValue;
}
