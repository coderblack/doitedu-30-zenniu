package cn.doitedu.rtmk.validate.benchmark;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class HbaseBenchMark {

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","doitedu");
        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf("zenniu_profile"));

        long start = System.currentTimeMillis();
        for(int i=0;i<1000;i++) {
            Get get = new Get(Bytes.toBytes(StringUtils.leftPad(RandomUtils.nextInt(1, 1000) + "", 6, "0")));
            Result result = table.get(get);
            result.getValue("f".getBytes(),("tag"+RandomUtils.nextInt(1,100)).getBytes());
            result.getValue("f".getBytes(),("tag"+RandomUtils.nextInt(1,100)).getBytes());
            result.getValue("f".getBytes(),("tag"+RandomUtils.nextInt(1,100)).getBytes());
        }

        long end = System.currentTimeMillis();

        System.out.printf("1000次查询，每次平均耗时为 : %d 毫秒 \n",(end-start)/1000);

        table.close();
        conn.close();
    }
}
