import DataStructs.DeviceTypeInformation;
import DataStructs.ZoneInformation;

import java.util.HashMap;
import java.util.Map;

public class StateCRDT {


    // Key -> AggregatorID. Value -> Information of the devices of that aggreaator.
    private Map<Integer, ZoneInformation> zoneInfo;


    public StateCRDT() {
        this.zoneInfo = new HashMap<>();
    }

    public void merge(StateCRDT received){
        for(Map.Entry<Integer, ZoneInformation> aux : received.zoneInfo.entrySet()){
            if (this.zoneInfo.containsKey(aux.getKey())) {
                this.zoneInfo.get(aux.getKey()).merge(aux.getValue());
            }
            else {
                this.zoneInfo.put(aux.getKey(), aux.getValue());
            }
        }
    }

    public boolean getIsDeviceOnline(int deviceId){
        return zoneInfo != null &&
                zoneInfo.values().stream()
                        .anyMatch(a -> a != null && a.checkDeviceOnline(deviceId));
    }
    public long getDevicesOnline(){
        return zoneInfo != null ?
                zoneInfo.values().stream()
                        .mapToLong(a -> a != null ? a.getOnlineDevices() : 0)
                        .sum() :
                0;
    }
    public long getDevicesOnlineOfType(String type){
        return zoneInfo != null ?
                zoneInfo.values().stream()
                        .mapToLong(a -> a != null ? a.getOnlineCounterDeviceType(type) : 0)
                        .sum() :
                0;
    }
    public int getNumberEvents(String eventType){
        return zoneInfo != null ?
                zoneInfo.values().stream()
                        .mapToInt(a -> a != null ? a.getEventCounter(eventType) : 0)
                        .sum() :
                0;
    }
}
