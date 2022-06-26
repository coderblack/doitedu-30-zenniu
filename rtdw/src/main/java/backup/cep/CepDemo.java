package backup.cep;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.row;

public class CepDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        tenv.createTable("t1", TableDescriptor.forConnector("filesystem")
                .format("csv")
                .schema(Schema.newBuilder()
                        .column("symbol", DataTypes.STRING())
                        .column("rowtime", DataTypes.TIMESTAMP(3))
                        .column("price", DataTypes.INT())
                        .column("tax", DataTypes.INT())
                        .watermark("rowtime", "rowtime")
                        .build())
                .option("path", "data/cep/stock.txt")
                .build()
        );

        tenv.executeSql(
                " SELECT *                                                                                       " +
                        " FROM t1                                                                                         " +
                        "     MATCH_RECOGNIZE (                                                                          " +
                        "         PARTITION BY symbol                                                                    " +
                        "         ORDER BY rowtime                                                                       " +
                        "         MEASURES                                                                               " +
                        "             START_ROW.rowtime AS start_tstamp,                                                 " +
                        "             LAST(PRICE_DOWN.rowtime) AS bottom_tstamp,                                         " +
                        "             LAST(PRICE_UP.rowtime) AS end_tstamp                                               " +
                        "         ONE ROW PER MATCH                                                                      " +
                        "         AFTER MATCH SKIP TO LAST PRICE_UP                                                      " +
                        "         PATTERN (START_ROW PRICE_DOWN+ PRICE_UP)                                               " +
                        "         DEFINE                                                                                 " +
                        "             PRICE_DOWN AS                                                                      " +
                        "                 (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < START_ROW.price) OR  " +
                        "                     PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1),                              " +
                        "             PRICE_UP AS                                                                        " +
                        "                 PRICE_UP.price > LAST(PRICE_DOWN.price, 1)                                     " +
                        "     ) MR                                                                                       "
        )/*.print()*/;

        /**
         *  -----------------------------------------
         */


        tenv.createTable("t2", TableDescriptor.forConnector("filesystem")
                .format("csv")
                .schema(Schema.newBuilder()
                        .column("symbol", DataTypes.STRING())
                        .column("rowtime", DataTypes.TIMESTAMP(3))
                        .column("price", DataTypes.INT())
                        .column("tax", DataTypes.INT())
                        .watermark("rowtime", "rowtime")
                        .build())
                .option("path", "data/cep/stock.2.txt")
                .build()
        );
        tenv.executeSql(
                " SELECT *                                        "+
                        " FROM t2                                          "+
                        "     MATCH_RECOGNIZE (                           "+
                        "         PARTITION BY symbol                     "+
                        "         ORDER BY rowtime                        "+
                        "         MEASURES                                "+
                        "             FIRST(A.rowtime) AS start_tstamp,   "+
                        "             LAST(A.rowtime) AS end_tstamp,      "+
                        "             AVG(A.price) AS avgPrice            "+
                        "         ONE ROW PER MATCH                       "+
                        "         AFTER MATCH SKIP PAST LAST ROW          "+
                        "         PATTERN (A+ B)                          "+
                        "         DEFINE                                  "+
                        "             A AS AVG(A.price) < 15              "+
                        "     ) MR                                        "
        )/*.print()*/;


        /**
         * --------------------------------------------------
         */
        tenv.createTable("t3", TableDescriptor.forConnector("filesystem")
                .format("csv")
                .schema(Schema.newBuilder()
                        .column("tid", DataTypes.STRING())
                        .column("rowtime", DataTypes.TIMESTAMP(3))
                        .column("eventid", DataTypes.STRING())
                        .watermark("rowtime", "rowtime")
                        .build())
                .option("path", "data/cep/ad.txt")
                .build()
        );

        tenv.executeSql(
                " SELECT *                                               "+
                        " FROM t3                                                "+
                        "     MATCH_RECOGNIZE (                                  "+
                        "         PARTITION BY tid                               "+
                        "         ORDER BY rowtime                               "+
                        "         MEASURES                                       "+
                        "             FIRST(A.tid) AS show_tid,                  "+
                        "             FIRST(A.eventid) AS show_event,            "+
                        "             FIRST(A.rowtime) AS show_time,             "+
                        "             FIRST(B.tid) AS c_tid,                     "+
                        "             FIRST(B.eventid) AS c_event,               "+
                        "             FIRST(B.rowtime) AS c_time                 "+
                        "         ONE ROW PER MATCH                              "+
                        "         AFTER MATCH SKIP PAST LAST ROW                 "+
                        "         PATTERN (A+ B) WITHIN INTERVAL '6' SECOND      "+
                        "         DEFINE                                         "+
                        "             A AS A.eventid='adshow'  ,                 "+
                        "             B AS B.tid=A.tid AND B.eventid='adclick'   "+
                        "     ) MR                                               "
        ).print();



    }
}
