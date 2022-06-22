package cn.doitedu.rtmk.validate;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Rule {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("", 9999);


        stream.keyBy("guid")
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String event, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                        // 将本条行为事件放入state中


                        // 规则计算



                        // 判断是否满足某个规则的触发条件



                        // 如果满足，则要去检查规则中的每个条件是否满足



                        // 判断完成


                        out.collect(event);


                    }
                });


    }


}
