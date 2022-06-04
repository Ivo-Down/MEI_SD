package DataStructs;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


public class ZoneInformation implements Serializable {

    private Map<String, Integer> eventCounter;   // Map that saves the nr of events of each type. Key -> Type ; Value -> Nr of events of that type.

    private Map<String, Integer> onlineRecord;   // Record of online devices of each type.
    private Map<String, Pair> onlineCounter;     // Nr of online devices in each type.

    private Map<Integer, Pair> onlineDevices;    // Map that saves the state of each device (online or offline).

    public ZoneInformation() {
        this.eventCounter = new HashMap<>();
        this.onlineRecord = new HashMap<>();
        this.onlineDevices = new HashMap<>();
        this.onlineCounter = new HashMap<>();
    }

    public boolean merge(ZoneInformation received){

        boolean res = false;
        // Percorrer eventCounter
        for (Map.Entry<String,Integer> event : received.eventCounter.entrySet()){
            if(this.eventCounter.containsKey(event.getKey())){
                if (event.getValue() > this.eventCounter.get(event.getKey())){
                    this.eventCounter.put(event.getKey(), event.getValue());
                    res = true;
                }

            } else {
                this.eventCounter.put(event.getKey(), event.getValue());
                res = true;
            }
        }

        // Percorrer onlineRecord
        for (Map.Entry<String,Integer> record : received.onlineRecord.entrySet()) {
            if(this.onlineRecord.containsKey(record.getKey())){
                if (record.getValue() > this.onlineRecord.get(record.getKey())){
                    this.onlineRecord.put(record.getKey(), record.getValue());
                    res = true;
                }

            } else {
                this.onlineRecord.put(record.getKey(), record.getValue());
                res = true;
            }
        }

        // Percorrer OnlineCounter
        for (Map.Entry<String,Pair> counter : received.onlineCounter.entrySet()) {
            if(this.onlineCounter.containsKey(counter.getKey())){
                Pair aux = this.onlineCounter.get(counter.getKey());
                int maxFst = Math.max(aux.getFst(), counter.getValue().getFst());
                int maxSnd = Math.max(aux.getSnd(), counter.getValue().getSnd());
                if (maxFst > aux.getFst() || maxSnd > aux.getSnd()){
                    this.onlineCounter.put(counter.getKey(), new Pair(maxFst, maxSnd));
                    res = true;
                }

            } else {
                this.onlineCounter.put(counter.getKey(), counter.getValue());
                res = true;
            }
        }

        for (Map.Entry<Integer,Pair> device : received.onlineDevices.entrySet()) {
            if(this.onlineDevices.containsKey(device.getKey())){
                Pair aux = this.onlineDevices.get(device.getKey());
                int maxFst = Math.max(aux.getFst(), device.getValue().getFst());
                int maxSnd = Math.max(aux.getSnd(), device.getValue().getSnd());
                if (maxFst > aux.getFst() || maxSnd > aux.getSnd()){
                    this.onlineDevices.put(device.getKey(), new Pair(maxFst, maxSnd));
                    res = true;
                }
            } else {
                this.onlineDevices.put(device.getKey(), device.getValue());
                res = true;
            }
        }

        return res;
    }


    /* - - - - - - - FUNÇÕES AUXILIARES - - - - - - - - */

    public Integer getEventCounter(String eventType){
        return this.eventCounter.get(eventType);
    }

    public Integer getOnlineRecordType(String deviceType){
        return this.onlineRecord.get(deviceType);
    }

    public Integer getOnlineCounterDeviceType(String deviceType){
        Pair p = this.onlineCounter.get(deviceType);
        return p.getFst() - p.getSnd();
    }

    public Integer getOnlineCounter(){
        int total = 0;
        for(Map.Entry<String, Pair> aux : onlineCounter.entrySet()) {
            Pair p = aux.getValue();
            total += p.getFst() - p.getSnd();
        }
        return total;
    }

    public Boolean checkDeviceOnline(Integer deviceID){
        Pair p = this.onlineDevices.get(deviceID);
        return p.getFst() > p.getSnd();
    }


    public void addEventCounter(String eventType){
        //this.eventCounter.putIfAbsent(eventType, 0);
        //this.eventCounter.put(eventType, this.eventCounter.get(eventType)+1);

        this.eventCounter.merge(eventType, 1, Integer::sum); // TODO: Testar se funfa lmao. Se nao funfar, usar as 2 de cima.
    }

}
