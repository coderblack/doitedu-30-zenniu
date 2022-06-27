package cepdemos;

import io.netty.handler.codec.http2.Http2Exception;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/6/27
 * @Desc: 学大数据，到多易教育
 *
 *  识别股票价格序列中的各个波段底部
 **/
public class StockPriceBottom {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        tenv.createTable("stock", TableDescriptor.forConnector("filesystem")
                        .format("csv")
                        .schema(Schema.newBuilder()
                                .column("symbol", DataTypes.STRING())
                                .column("rt", DataTypes.TIMESTAMP(3))
                                .column("price", DataTypes.DOUBLE())
                                .column("tax", DataTypes.DOUBLE())
                                .watermark("rt","rt")
                                .build())
                        .option("path","data/cep/stock.txt")
                .build());

        //tenv.executeSql("select * from stock").print();

        // 识别波段底部，用CEP
        tenv.executeSql("SELECT *\n" +
                "FROM stock\n" +
                "    MATCH_RECOGNIZE (\n" +
                "        PARTITION BY symbol\n" +
                "        ORDER BY rt\n" +
                "        MEASURES\n" +
                "            START_ROW.rt AS start_tstamp,\n" +
                "            LAST(PRICE_DOWN.rt) AS bottom_tstamp,\n" +
                "            LAST(PRICE_UP.rt) AS end_tstamp\n" +
                "        ONE ROW PER MATCH\n" +
                "        AFTER MATCH SKIP TO LAST PRICE_UP\n" +
                "        PATTERN (START_ROW PRICE_DOWN+ PRICE_UP)\n" +
                "        DEFINE\n" +
                "            PRICE_DOWN AS\n" +
                "                (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < START_ROW.price) OR\n" +
                "                    PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1),\n" +
                "            PRICE_UP AS\n" +
                "                PRICE_UP.price > LAST(PRICE_DOWN.price, 1)\n" +
                "    ) MR").print();



    }

}
