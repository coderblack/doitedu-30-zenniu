package cn.doitedu.rtmk.validate.utils;

import cn.doitedu.rtmk.validate.pojo.EventUnitCondition;
import cn.doitedu.rtmk.validate.pojo.UserMallEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/***
 * @author hunter.d
 * @qq 657270652
 * @wx haitao-duan
 * @date 2021/7/25
 **/

@Slf4j
public class EventUtils {

    public static String eventSeq2Str(Iterator<UserMallEvent> eventSeq, List<EventUnitCondition> eventConditionList){
        StringBuilder sb = new StringBuilder();
        while(eventSeq.hasNext()){
            UserMallEvent next = eventSeq.next();
            for(int i=1;i<=eventConditionList.size();i++)
            if(eventMatchCondition(next,eventConditionList.get(i-1))) sb.append(i);
        }
        return sb.toString();
    }

    public static int sequenceStrMatchRegexCount(String eventStr, String pattern){
        Pattern r = Pattern.compile(pattern);
        Matcher matcher = r.matcher(eventStr);
        int count = 0;
        while(matcher.find()) count ++;
        //log.debug("字符串正则匹配,正则表达式：{}, 匹配结果为：{} ,字符串为：{} ",pattern,count,eventStr);
        return count;
    }

    public static boolean eventMatchCondition(UserMallEvent bean, EventUnitCondition eventCondition){
        if (bean.getEventId().equals(eventCondition.getEventId())) {
            Set<String> keys = eventCondition.getEventProps().keySet();
            for (String key : keys) {
                String conditionValue = eventCondition.getEventProps().get(key);
                if (!conditionValue.equals(bean.getProperties().get(key))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
