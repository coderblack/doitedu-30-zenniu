package cn.doitedu.rtmk.validate.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2021/7/26
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketingRule {
    // 规则ID
    private String ruleId;

    // keyby的字段, comma seperated  "province,city"  | "guid"
    private String keyByFields;

    // 触发事件
    private EventUnitCondition triggerEventCondition;

    // 画像属性条件
    private Map<String,String> userProfileConditions;

    // 行为组合条件
    private List<EventComposeCondition> eventComposeConditionList;

    // 规则匹配推送次数限制
    private int matchLimit;

    // 是否要注册timer
    private boolean onTimer;

    // 定时条件时间
    private List<TimerCondition> timerConditionList;

}
