package groovy.demo;

import java.awt.*;
import java.sql.*;
import java.util.LinkedList;

public class Main {
    public static void main(String[] args) throws SQLException {

/*        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk", "root", "root");

        Statement stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery("select ruleId,rule_match_state_impl from rule_info where ruleId = 3");


        resultSet.next();
        String javaCode = resultSet.getString(2);*/

        // 怎么调数据库中那段java代码


        LinkedList<String> lst = new LinkedList<>();
        lst.add("a");
        lst.add("b");
        lst.add("c");
        lst.removeFirst();
        lst.add("d");

        System.out.println(lst);


    }
}
