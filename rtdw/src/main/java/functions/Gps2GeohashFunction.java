package functions;

import ch.hsr.geohash.GeoHash;
import com.ibm.icu.impl.Grego;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import pojo.EventBean;

public class Gps2GeohashFunction extends ProcessFunction<EventBean,EventBean> {
    @Override
    public void processElement(EventBean eventBean, ProcessFunction<EventBean, EventBean>.Context ctx, Collector<EventBean> out) throws Exception {
        Double lat = eventBean.getLatitude();
        Double lng = eventBean.getLongitude();

        if(lat != null && lng != null && lat<90 && lat>-90 && lng>-180 && lng<180){

            String geohash = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 6);
            eventBean.setGeoHashCode(geohash);
        }

        out.collect(eventBean);

    }
}
