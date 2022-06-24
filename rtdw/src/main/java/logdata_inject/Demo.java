package logdata_inject;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        HashMap<String, String> mp1 = new HashMap<>();
        mp1.put("a","1");
        DataStreamSource<B> ds = env.fromCollection(Arrays.asList(new B(mp1)));
        tenv.createTemporaryView("v",ds);

        tenv.executeSql("desc v").print();

        //tenv.executeSql("select cast( f0 as map<string,string> ) as m from v ").print();


        env.execute();

    }

    public static class B{

        @DataTypeHint(value = "map" ,bridgedTo = java.util.Map.class)
        public Map<String,String> mp;

        public B(Map<String, String> mp) {
            this.mp = mp;
        }

        public B() {
        }
    }
}
