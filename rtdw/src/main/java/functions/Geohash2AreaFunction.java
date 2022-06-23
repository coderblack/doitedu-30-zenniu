package functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import pojo.EventBean;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Geohash2AreaFunction extends KeyedProcessFunction<String, EventBean,EventBean> {
    Connection conn;
    PreparedStatement preparedStatement;
    @Override
    public void open(Configuration parameters) throws Exception {

        conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk","root","root");
        preparedStatement = conn.prepareStatement("select province,city,region from ref_geohash where geohash = ?");

    }

    @Override
    public void processElement(EventBean eventBean, KeyedProcessFunction<String, EventBean, EventBean>.Context ctx, Collector<EventBean> out) throws Exception {

        String geoHashCode = eventBean.getGeoHashCode();


        String province = null;
        String city = null;
        String region = null;

        if(StringUtils.isNotBlank(geoHashCode)){
            preparedStatement.setString(1,geoHashCode);
            ResultSet resultSet = preparedStatement.executeQuery();

            while(resultSet.next()){
                province = resultSet.getString(1);
                city = resultSet.getString(2);
                region = resultSet.getString(3);
            }

            eventBean.setProvince(province);
            eventBean.setCity(city);
            eventBean.setRegion(region);
        }

        if(province == null) {
            // 侧流输出这个用户的gps坐标
            ctx.output(new OutputTag<String>("unknown_gps", TypeInformation.of(String.class)),eventBean.getLatitude()+","+ eventBean.getLongitude());
        }


        // 输出主流数据
        out.collect(eventBean);

    }

    @Override
    public void close() throws Exception {

        preparedStatement.close();
        conn.close();
    }
}
