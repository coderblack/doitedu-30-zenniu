package prepare;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RuleMgmtTableRecord {
    // 规则id
    private String id;
    // 规则名称
    private String rule_name;
    // 规则条件json
    private String rule_condition_json;
    // 规则controller动态逻辑drools模板
    private String rule_controller_drl;
    // 规则状态 =>:  0 未审核, 1 已审核, 2 上线, 3 下线
    private String rule_status;
    // 规则创建时间
    private String create_time;
    // 规则更新时间
    private String modify_time;
    // 规则发布人
    private String publiser;
}