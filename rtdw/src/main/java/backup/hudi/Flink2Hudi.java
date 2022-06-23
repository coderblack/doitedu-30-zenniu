package backup.hudi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.row;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/22
 * @Desc:  核心参数说明
 *   table.type: 测试中我们使用COPY_ON_WRITE。如果使用MERGE_ON_READ，在生成parquet文件之前，Hive查询不到数据
 *   hive_sync.enable: 是否启用hive同步
 *   hive_sync.mode: hive同步模式，包含hms和jdbc两种，这里使用hms模式
 *   hive_sync.metastore.uris: 配置hive metastore的URI
 *   hive_sync.table: 同步到hive中的表名称
 *   hive_sync.db: 同步到hive的哪个数据库中
 * 在beeline中查询同步到hive表，按官网所说，需要设置参数：
 *     hive.input.format=org.apache.hudi.hadoop.HoodieParquetInputFormat
 *
 * 而实际上，测试时用hive默认的inputFormat似乎也可以
 *     hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat
 *
 * 对MOR表，用flink任务做离线压缩
 *   ./bin/flink run -c org.apache.hudi.sink.compact.HoodieFlinkCompactor lib/hudi-flink-bundle_2.11-0.9.0.jar --path hdfs://xxx:9000/table
 *
 *
 **/
public class Flink2Hudi {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStreamSource<Bean> s = env.addSource(new MySource());
        tenv.createTemporaryView("t_1",s);


        // tenv.executeSql("select * from t_1").print();
        // System.exit(1);

        tenv.executeSql(
                "CREATE TABLE flink_ht (                                                \n" +
                        "  guid bigint ,                                                             \n" +
                        "  eventid string,                                                           \n" +
                        "  ts bigint,                                                                \n" +
                        "  dt string,                                                                \n" +
                        "  primary key(guid,eventid) not enforced  --必须指定主键                     \n" +
                        ")                                                                         \n" +
                        "PARTITIONED BY (dt)                                                       \n" +
                        "with(                                                                     \n" +
                        "'connector'='hudi'                                                       \n" +
                        ", 'path'= 'hdfs://doitedu:8020/huditable/flink_ht'                      \n" +
                        ", 'hoodie.datasource.write.recordkey.field'= 'guid，eventid'  -- 主键     \n" +
                        ", 'write.precombine.field'= 'ts'  -- 自动precombine的字段                   \n" +
                        ", 'write.tasks'= '1'                                                      \n" +
                        ", 'compaction.tasks'= '1'                                                 \n" +
                        ", 'write.rate.limit'= '2000'  -- 限速                                       \n" +
                        ", 'table.type'= 'MERGE_ON_READ'  -- 默认COPY_ON_WRITE,可选MERGE_ON_READ     \n" +
                        ", 'compaction.async.enabled'= 'true'  -- 是否开启异步压实                  \n" +
                        ", 'compaction.trigger.strategy'= 'num_commits'  -- 异步压实按delta commit触发        \n" +
                        ", 'compaction.delta_commits'= '5'  -- 默认为5                               \n" +
                        ", 'changelog.enabled'= 'true'  -- 开启changelog变更                         \n" +
                        ", 'read.streaming.enabled'= 'true' -- 开启流读                             \n" +
                        ", 'read.streaming.check-interval'= '3'  -- 检查间隔，默认60s                \n" +
                        ", 'hive_sync.enable'= 'true' -- 开启自动同步hive                           \n" +
                        ", 'hive_sync.mode'= 'hms' -- 自动同步hive模式，默认jdbc模式                \n" +
                        ", 'hive_sync.metastore.uris'= 'thrift://doitedu:9083'-- hive metastore地址\n" +
                        "  -- , 'hive_sync.jdbc_url'= 'jdbc:hive2://hadoop:10000'-- hiveServer地址 \n" +
                        ", 'hive_sync.table'= 'flink_ht'  -- hive 新建表名                       \n" +
                        ", 'hive_sync.db'= 'huditable'  -- hive 新建数据库名                         \n" +
                        ", 'hive_sync.username'= ''  -- HMS 用户名                                   \n" +
                        ", 'hive_sync.password'= ''  -- HMS 密码                                     \n" +
                        ", 'hive_sync.support_timestamp'= 'true'  -- 兼容hive timestamp类型   \n" +
                        ")       \n"

        );

        //tenv.executeSql("select * from flink_hive01").print();

        tenv.executeSql("insert into flink_ht select * from t_1");


        tenv.executeSql("select * from flink_ht").print();

        env.execute();


    }

    public static class MySource implements SourceFunction<Bean>{

        @Override
        public void run(SourceContext<Bean> ctx) throws Exception {
            int i= 0;
            while(true){
                i++;
                ctx.collect(new Bean(i,"e"+i,i*1000,"2022-06-22"));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {

        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean{
        private int guid;
        private String eventid;
        private long ts;
        private String dt;
    }
}
