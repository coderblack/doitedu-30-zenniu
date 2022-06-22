package groovy.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventBean{
    private String guid;
    private String eventId;
    private int timeLong;
}
