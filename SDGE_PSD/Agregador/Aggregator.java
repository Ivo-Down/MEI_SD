import DataStructs.DeviceTypeInformation;

import java.util.HashMap;
import java.util.Map;

public class Aggregator {
    private final String zoneName;
    private final int id;

    // Key -> AggregatorID. Value -> Information of the devices of that aggreaator.
    private Map<Integer, Map<String, DeviceTypeInformation>> struct;

    // Map that saves the nr of events of each type. Key -> Type;
    private Map<String, Integer> eventsCounter;

    public Aggregator(String zoneName, int id){
        this.zoneName = zoneName;
        this.id = id;
        this.struct = new HashMap<>();
        this.eventsCounter = new HashMap<>();
    }
    public String getzoneName(){
        return zoneName;
    }

    public int getId(){
        return id;
    }
}
