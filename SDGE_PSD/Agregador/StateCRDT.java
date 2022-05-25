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
}
