package cn.doitedu.datagen.main;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/18
 * @Desc: 用户画像模拟数据生成器
 *
 *
 *
 *
 **/
public class UserTagsGen {

    public static void main(String[] args) throws IOException {

        // hbase的zookeeper地址
        String zkHost = "doitedu:2181";
        // hbase中的画像表名称
        String tableName = "zenniu_profile";
        // 需要生成的用户数
        int userCount = 1000;

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", zkHost);

        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf(tableName));

        ArrayList<Put> puts = new ArrayList<>();
        for (int i = 1; i <= userCount; i++) {

            // 生成一个用户的画像标签数据
            String deviceId = StringUtils.leftPad(i + "", 6, "0");
            Put put = new Put(Bytes.toBytes(deviceId));
            for (int k = 1; k <= 100; k++) {
                String key = "tag" + k;
                String value = "v" + RandomUtils.nextInt(1, 3);
                put.addColumn(Bytes.toBytes("f"), Bytes.toBytes(key), Bytes.toBytes(value));
            }

            // 将这一条画像数据，添加到list中
            puts.add(put);

            // 攒满100条一批
            if(puts.size()==100) {
                table.put(puts);
                puts.clear();
            }

        }

        // 提交最后一批
        if(puts.size()>0) table.put(puts);

        conn.close();
    }

}
