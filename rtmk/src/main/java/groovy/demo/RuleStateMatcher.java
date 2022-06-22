package groovy.demo;

import org.apache.flink.api.common.state.ListState;

public interface RuleStateMatcher {

    boolean matchRuleInState(String str);

    boolean matchRuleInState(ListState<EventBean> eventState);
}
