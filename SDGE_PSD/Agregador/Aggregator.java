import DataStructs.DeviceTypeInformation;
import DataStructs.ZoneInformation;

import java.util.HashMap;
import java.util.Map;

public class Aggregator {
    private final String zoneName;
    private final int id;

    // Key -> AggregatorID. Value -> Information of the devices of that aggregator.
    private Map<Integer, Map<String, DeviceTypeInformation>> struct;

    // Map that saves the nr of events of each type. Key -> Type;
    private Map<String, Integer> eventsCounter;
    private StateCRDT stateInfo;


    public Aggregator(String zoneName, int id){
        this.zoneName = zoneName;
        this.id = id;
        this.stateInfo = new StateCRDT();
    }
    public String getzoneName(){
        return zoneName;
    }

    public int getId(){
        return id;
    }

    public boolean getIsDeviceOnline(int deviceId){
        return struct != null &&
                struct.values().stream()
                    .anyMatch(a -> a != null &&
                            a.values().stream()
                                    .anyMatch(b -> b.isDeviceOnline(deviceId)));
    }
    public long getDevicesOnline(){
        return struct != null ?
                struct.values().stream()
                        .mapToLong(a -> a != null ? a.values().stream().mapToLong(DeviceTypeInformation::getOnline).sum() : 0)
                        .sum() :
                0;
    }
    public long getDevicesOnlineOfType(String type){
        return struct != null ?
                struct.values().stream()
                        .mapToLong(a -> a != null ? a.get(type).getOnline() : 0)
                        .sum() :
                0;
    }
    public int getNumberEvents(String eventType){
        return eventsCounter != null ? eventsCounter.getOrDefault(eventType, 0) : 0;
    }
}
