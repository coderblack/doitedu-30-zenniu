package groovy.demo

import org.apache.flink.api.common.state.ListState

class EventCountAndTimeMaxRuleMatcher implements RuleStateMatcher{
    @Override
    boolean matchRuleInState(String str) {
        return false
    }

    @Override
    boolean matchRuleInState(ListState<EventBean> eventState) {

        Iterable<EventBean> iterable = eventState.get()
        int cnt = 0;
        int maxTimeLong = 0;
        for (EventBean eventBean : iterable) {
            //println("in rule groovy matcher , got an event: " + eventBean.getEventId())

            if(eventBean.getEventId().equals("e03")) {
                cnt ++;
                if(eventBean.getTimeLong()> maxTimeLong) maxTimeLong = eventBean.getTimeLong();
            }
        }

       // println("this matching ,result :  " + cnt + " , " + maxTimeLong)
        return cnt>=3 && maxTimeLong>800
    }
}
