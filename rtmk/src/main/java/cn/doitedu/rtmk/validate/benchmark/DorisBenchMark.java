package cn.doitedu.rtmk.validate.benchmark;


import org.apache.commons.lang3.RandomUtils;

import java.sql.*;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/19
 * @Desc: 学大数据，到多易教育
 *    doris 过滤点查性能测试
 **/
public class DorisBenchMark {

    public static void main(String[] args) throws SQLException {

        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:9030/rtmk", "root", "");
        PreparedStatement ps = conn.prepareStatement("select guid,eventid,propsJson  from dwd_mall_app_events where guid= ? and eventid= ?");

        long start = System.currentTimeMillis();
        for(int i=0;i<10000;i++){

            ps.setLong(1, RandomUtils.nextLong(1,350));
            ps.setString(2,"e_oh_3");

            ResultSet resultSet = ps.executeQuery();
            while(resultSet.next()){
                long guid = resultSet.getLong("guid");
                String eventid = resultSet.getString("eventid");
                String propsJson = resultSet.getString("propsJson");

            }
        }
        long end = System.currentTimeMillis();

        System.out.println((end-start)/1000.0);
    }
}
