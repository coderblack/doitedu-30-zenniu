package cn.doitedu.rtdw.log_etl.pojo;


import lombok.*;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class AreaBean implements Serializable {

    private String geoHashCode;
    private String province;
    private String city;
    private String region;
}
