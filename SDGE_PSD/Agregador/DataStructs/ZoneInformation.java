package DataStructs;

import com.ericsson.otp.erlang.OtpErlangObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ZoneInformation implements Serializable {

    private final Map<String, Integer> eventCounter;   // Map that saves the nr of events of each type. Key -> Type ; Value -> Nr of events of that type.

    private final Map<String, Integer> onlineRecord;   // Record of online devices of each type.

    private final Map<String, Pair> onlineCounter;     // Nr of online devices in each type.

    private final Map<Integer, Pair> onlineDevices;    // Map that saves the state of each device (online or offline).

    public ZoneInformation() {
        this.eventCounter = new HashMap<>();
        this.onlineRecord = new HashMap<>();
        this.onlineDevices = new HashMap<>();
        this.onlineCounter = new HashMap<>();
    }

    public ZoneInformation(ZoneInformation old){
        this.eventCounter = new HashMap<>();
        this.onlineRecord = new HashMap<>();
        this.onlineDevices = new HashMap<>();
        this.onlineCounter = new HashMap<>();

        this.eventCounter.putAll(old.eventCounter);
        this.onlineRecord.putAll(old.onlineRecord);
        for(Map.Entry<String, Pair> entry: old.onlineCounter.entrySet()){
            this.onlineCounter.put(entry.getKey(),new Pair(entry.getValue()));
        }
        for(Map.Entry<Integer, Pair> entry: old.onlineDevices.entrySet()){
            this.onlineDevices.put(entry.getKey(),new Pair(entry.getValue()));
        }
    }

    public Map<String, Integer> getOnlineRecord() {
        return onlineRecord;
    }

    public boolean merge(ZoneInformation received){

        boolean res = false;
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
        Integer res = this.eventCounter.get(eventType);
        if (res == null) return 0;
        else {
            return this.eventCounter.get(eventType);
        }
    }

    public List<String> getOnlineTypes(){
        List<String> types = new ArrayList<>();
        for(String type: this.onlineCounter.keySet()){
            if(getOnlineCounterDeviceType(type) > 0){
                types.add(type);
            }
        }
        return types;
    }

    public Integer getOnlineCounterDeviceType(String deviceType){
        Pair p = this.onlineCounter.get(deviceType);
        if(p == null) return 0;
        return p.getFst() - p.getSnd();
    }

    public Integer getOnlineCounter() {
        return this.onlineCounter.values().stream()
                .mapToInt(a -> a==null ? 0: a.getFst() - a.getSnd())
                .sum();
    }

    public Boolean checkDeviceOnline(Integer deviceID){
        Pair p = this.onlineDevices.get(deviceID);
        if(p == null) return false;
        return p.getFst() > p.getSnd();
    }

    public void addEvents(List<OtpErlangObject> eventsList){
        for(OtpErlangObject e: eventsList){
            String event = e.toString();

            if (this.eventCounter.containsKey(event))
                this.eventCounter.put(event, this.eventCounter.get(event) + 1);
            else
                this.eventCounter.put(event, 1);
        }
    }

    public boolean updateDeviceState(Integer deviceId, Boolean deviceState, String deviceType){
        boolean res = false;

        Pair pOnline = this.onlineDevices.get(deviceId);
        Pair pCounter = this.onlineCounter.get(deviceType);

        if(pOnline == null){
            this.onlineDevices.put(deviceId, new Pair());
            pOnline = this.onlineDevices.get(deviceId);
        }
        if(pCounter == null){
            this.onlineCounter.put(deviceType, new Pair());
            this.eventCounter.put(deviceType, 0);
            this.onlineRecord.put(deviceType, 0);
            pCounter = this.onlineCounter.get(deviceType);
        }

        // check device state
        boolean state = pOnline.getPairValue() > 0;

        if(state != deviceState){
            res = true;
            if (deviceState) {
               pOnline.addToFst(1);
               pCounter.addToFst(1);

                int pCounterValue = pCounter.getPairValue();

               if(pCounterValue > this.onlineRecord.get(deviceType)){
                   // Se o nr de dispositivos online for maior que o record, adicionar
                   this.onlineRecord.put(deviceType, pCounterValue);
               }
            }
            else {
                pOnline.addToSnd(1);
                pCounter.addToSnd(1);
            }

            this.onlineDevices.put(deviceId, pOnline);
            this.onlineCounter.put(deviceType, pCounter);
        }

        return res;
    }

}
