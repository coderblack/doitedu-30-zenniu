package cn.doitedu.rtmk.validate.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/18
 * @Desc: 事件组合体条件封装  类似于： [C !W F G](>=2)  [A.*B.*C]
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventComposeCondition {

    /**
     * 组合条件的发生时间区间起始
     */
    private long timeRangeStart;

    /**
     * 组合条件的发生时间区间结束
     */
    private long timeRangeEnd;

    /**
     * 组合发生的最小次数
     */
    private int minLimit;

    /**
     * 组合发生的最大次数
     */
    private int maxLimit;

    /**
     * 组合条件中关心的事件的列表
     * [{a,p1=v1}{y,p4=v2} !{c,p2=v1} {d,p3=p4,2=v8}]
     */
    private List<EventUnitCondition> eventConditionList;

    /**
     * 组合条件未来计算要用的正则匹配表达式
     */
    private String matchPattern;


    /**
     * 用于在数据库中过滤关心事件的sql
     */
    private String sqlType;
    private String querySql;


    /**
     * 条件缓存 ID
     */
    private String cacheId;
}
