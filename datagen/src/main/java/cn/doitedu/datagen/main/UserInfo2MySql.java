package cn.doitedu.datagen.main;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;

public class UserInfo2MySql {

    public static void main(String[] args) throws Exception {

        BufferedReader br = new BufferedReader(new FileReader("data/users/20220623.113713.dat"));

        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk", "root", "root");
        PreparedStatement preparedStatement = conn.prepareStatement("insert into ums_member (account,register_time) values (?,?)");

        String line = null;
        while( (line=br.readLine())!=null ){
            JSONObject jsonObject = JSON.parseObject(line);
            String account = jsonObject.getString("account");
            preparedStatement.setString(1,account);
            preparedStatement.setTimestamp(2,new Timestamp(System.currentTimeMillis()));
            preparedStatement.execute();
        }

        br.close();
        conn.close();

    }
}
