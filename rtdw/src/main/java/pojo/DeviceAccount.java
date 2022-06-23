package pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeviceAccount implements Comparable<DeviceAccount> {

    private String account;
    private Double score;
    private long bindTime;


    @Override
    public int compareTo(DeviceAccount o) {
        return Double.compare(o.getScore(),this.score);
    }
}
