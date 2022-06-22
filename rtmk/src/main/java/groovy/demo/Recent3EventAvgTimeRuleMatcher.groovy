package groovy.demo

import org.apache.flink.api.common.state.ListState

class Recent3EventAvgTimeRuleMatcher implements RuleStateMatcher{
    @Override
    boolean matchRuleInState(String str) {
        return false
    }

    @Override
    boolean matchRuleInState(ListState<EventBean> eventState) {


        LinkedList<EventBean> lst = new LinkedList<>();

        Iterable<EventBean> iterable = eventState.get()

        for (EventBean eventBean : iterable) {
            if(lst.size()>=3) lst.removeFirst()
            lst.add(eventBean)
        }

        int cnt = 0;
        int timeLongSum = 0;
        for(EventBean bean : lst){
            cnt++;
            timeLongSum += bean.getTimeLong();
        }

       // println("this matching ,result :  " + cnt + " , " + maxTimeLong)
        return (double)timeLongSum/cnt >= 400
    }
}
