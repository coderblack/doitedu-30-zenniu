package cn.doitedu.rtmk.validate.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Generated;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class KeyTagedUserMallEvent {
    private String keyTagValue;
    private UserMallEvent userMallEvent;
}
