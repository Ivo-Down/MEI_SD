import DataStructs.ZoneInformation;
import com.ericsson.otp.erlang.OtpErlangObject;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class StateCRDT  implements Serializable {


    // Key -> AggregatorID. Value -> Information of the devices of that aggreaator.
    private final Map<Integer, ZoneInformation> zoneInfo;
    private final Lock lock;

    // TODO: Implementar locks nisto! -> Done

    public StateCRDT() {
        this.zoneInfo = new HashMap<>();
        this.lock = new ReentrantLock();
    }

    public StateCRDT(int id) {
        this.zoneInfo = new HashMap<>();
        this.zoneInfo.put(id, new ZoneInformation());
        this.lock = new ReentrantLock();
    }

    public StateCRDT(StateCRDT old){
        this.zoneInfo = new HashMap<>();
        try{
            old.lock.lock();
            for(Integer zone: old.zoneInfo.keySet()){
                zoneInfo.put(zone, new ZoneInformation(old.zoneInfo.get(zone)));
            }
        } finally {
            old.lock.unlock();
        }

        this.lock = new ReentrantLock();
    }

    public String toString(){
        return "olá";
    }


    public byte[] serialize() {
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(boas)){
            try {
                this.lock.lock();
                oos.writeObject(this);
            } finally {
                this.lock.unlock();
            }
            oos.flush();
            oos.close();
            return boas.toByteArray();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        throw new RuntimeException();
    }

    public boolean merge(StateCRDT received){
        try {
            this.lock.lock();
            boolean res = false;
            for(Map.Entry<Integer, ZoneInformation> aux : received.zoneInfo.entrySet()){
                if (this.zoneInfo.containsKey(aux.getKey())) {
                    res = this.zoneInfo.get(aux.getKey()).merge(aux.getValue());

                }
                else {
                    this.zoneInfo.put(aux.getKey(), aux.getValue());
                    res = true;
                }
            }
            return res;
        } finally {
            this.lock.unlock();
        }

    }


    public int getNumberEvents(String eventType) {
        try {
            this.lock.lock();
            return zoneInfo != null ?
                    zoneInfo.values().stream()
                            .mapToInt(a -> a != null ? a.getEventCounter(eventType) : 0)
                            .sum() :
                    0;
        } finally {
            this.lock.unlock();
        }
    }


    public int getDevicesOnlineOfType(String deviceType) {
        try {
            this.lock.lock();
            return zoneInfo != null ?
                    zoneInfo.values().stream()
                            .mapToInt(a -> a != null ? a.getOnlineCounterDeviceType(deviceType) : 0)
                            .sum() :
                    0;
        } finally {
            this.lock.unlock();
        }
    }


    public boolean getIsDeviceOnline(int deviceId) {
        try {
            this.lock.lock();
            return zoneInfo != null &&
                    zoneInfo.values().stream()
                            .anyMatch(a -> a != null && a.checkDeviceOnline(deviceId));
        } finally {
            this.lock.unlock();
        }
    }

    public Map<String, Integer> getRecords(Integer agg){
        try {
            this.lock.lock();
            return zoneInfo.get(agg).getOnlineRecord();
        } finally {
            this.lock.unlock();
        }
    }

    public int getDevicesOnline(){
        try {
            this.lock.lock();
            return zoneInfo != null ?
                    zoneInfo.values().stream()
                            .mapToInt(a -> a != null ? a.getOnlineCounter() : 0)
                            .sum() :
                    0;
        } finally {
            this.lock.unlock();
        }
    }


    public void addEvents(List<OtpErlangObject> eventsList, Integer zoneId){
        try {
            this.lock.lock();
            this.zoneInfo.get(zoneId).addEvents(eventsList);
        } finally {
            this.lock.unlock();
        }
    }


    public boolean updateDeviceState(Integer deviceId, Boolean deviceState, String deviceType, Integer zoneId){
        try {
            this.lock.lock();
            System.out.println();
            return this.zoneInfo.get(zoneId).updateDeviceState(deviceId, deviceState, deviceType);
        } finally {
            this.lock.unlock();
        }
    }

    //* NOTIFICATIONS CHECK *//
    public List<String> checkTypesWithOnlineDevices(Integer aggID){
        try {
            this.lock.lock();
            ZoneInformation thisZoneInfo = this.zoneInfo.get(aggID);
            return thisZoneInfo.getOnlineTypes();
        } finally {
            this.lock.unlock();
        }
    }
    public Integer getOnlinePercentage(Integer agg){
        try {
            this.lock.lock();
            var zoneOnline = zoneInfo.get(agg).getOnlineCounter();
            var totalOnline = this.getDevicesOnline();
            Integer percentage = Math.round(((float)zoneOnline / (float) totalOnline)*100);
            return percentage;
        } finally {
            this.lock.unlock();
        }
    }
}
