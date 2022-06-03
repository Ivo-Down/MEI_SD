import DataStructs.DeviceTypeInformation;
import DataStructs.ZoneInformation;

import java.util.HashMap;
import java.util.Map;

public class StateCRDT {


    // Key -> AggregatorID. Value -> Information of the devices of that aggreaator.
    private Map<Integer, ZoneInformation> zoneInfo;

    // TODO: Implementar locks nisto!


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


    public int getNumberEvents(String eventType){
        int eventsNumber = 0;
        for(Map.Entry<Integer, ZoneInformation> aux : zoneInfo.entrySet())
            eventsNumber += aux.getValue().getEventCounter(eventType);

        return eventsNumber;
    }


    public int getDevicesOnlineOfType(String deviceType){
        int devicesNumber = 0;
        for(Map.Entry<Integer, ZoneInformation> aux : zoneInfo.entrySet())
            devicesNumber += aux.getValue().getOnlineCounterDeviceType(deviceType);

        return devicesNumber;
    }


    public boolean getIsDeviceOnline(int deviceId){
        for(Map.Entry<Integer, ZoneInformation> aux : zoneInfo.entrySet())
            if (aux.getValue().checkDeviceOnline(deviceId))
                return true;

        return false;
    }


    public int getDevicesOnline(){
        int devicesNumber = 0;
        for(Map.Entry<Integer, ZoneInformation> aux : zoneInfo.entrySet())
            devicesNumber += aux.getValue().getOnlineCounter();

        return devicesNumber;
    }
}
