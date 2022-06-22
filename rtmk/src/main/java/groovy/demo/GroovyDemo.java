package groovy.demo;

import groovy.lang.GroovyClassLoader;

import java.sql.*;

public class GroovyDemo {
    public static void main(String[] args) throws Exception {

        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk", "root", "root");

        Statement stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery("select ruleId,rule_match_state_impl from rule_info");
        while(resultSet.next()){
            long ruleId = resultSet.getLong(1);
            String ruleMatchImplCode = resultSet.getString(2);


            /**
             *  这段代码是为了做什么？
             */
            GroovyClassLoader groovyClassLoader = new GroovyClassLoader();


            Class<?> aClass1 = Class.forName("org.apache.Bean");
            Object o1 = aClass1.newInstance();


            Class aClass = groovyClassLoader.parseClass(ruleMatchImplCode);
            RuleStateMatcher o = (RuleStateMatcher) aClass.newInstance();

            boolean result = o.matchRuleInState("cccc");

            System.out.printf("规则 %d , 计算结果是： %s  \n",ruleId,result);

        }
    }
}
