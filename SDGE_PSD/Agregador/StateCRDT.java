import DataStructs.DeviceTypeInformation;
import DataStructs.ZoneInformation;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class StateCRDT  implements Serializable {


    // Key -> AggregatorID. Value -> Information of the devices of that aggreaator.
    private Map<Integer, ZoneInformation> zoneInfo;
    private Lock l;

    // TODO: Implementar locks nisto!


    public StateCRDT() {
        this.zoneInfo = new HashMap<>();
        this.l = new ReentrantLock();
    }


    public String toString(){
        return "olá";
    }


    public byte[] serialize() {
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(boas)){
            try {
                this.l.lock();
                oos.writeObject(this.zoneInfo);
            } finally {
                this.l.unlock();
            }
            oos.flush();
            oos.close();
            return boas.toByteArray();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        throw new RuntimeException();
    }

    public static Object deserialize(byte[] bytes) {
        InputStream is = new ByteArrayInputStream(bytes);
        try (ObjectInputStream ois = new ObjectInputStream(is)) {
            return ois.readObject();
        } catch (IOException | ClassNotFoundException ioe) {
            ioe.printStackTrace();
        }
        throw new RuntimeException();
    }

    public boolean merge(StateCRDT received){
        try {
            this.l.lock();
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
            this.l.unlock();
        }

    }


    public int getNumberEvents(String eventType){
        try {
            this.l.lock();
            int eventsNumber = 0;
            for(Map.Entry<Integer, ZoneInformation> aux : zoneInfo.entrySet())
                eventsNumber += aux.getValue().getEventCounter(eventType);
            return eventsNumber;
        } finally {
            this.l.unlock();
        }
    }


    public int getDevicesOnlineOfType(String deviceType){
        try {
            this.l.lock();
            int devicesNumber = 0;
            for(Map.Entry<Integer, ZoneInformation> aux : zoneInfo.entrySet())
                devicesNumber += aux.getValue().getOnlineCounterDeviceType(deviceType);

            return devicesNumber;
        } finally {
            this.l.unlock();
        }

    }


    public boolean getIsDeviceOnline(int deviceId){
        try {
                this.l.lock();
                for(Map.Entry<Integer, ZoneInformation> aux : zoneInfo.entrySet())
                    if (aux.getValue().checkDeviceOnline(deviceId))
                        return true;

                return false;
            } finally {
                this.l.unlock();
        }


    }


    public int getDevicesOnline(){
        try {
            this.l.lock();
            int devicesNumber = 0;
            for(Map.Entry<Integer, ZoneInformation> aux : zoneInfo.entrySet())
                devicesNumber += aux.getValue().getOnlineCounter();

            return devicesNumber;
        } finally {
            this.l.unlock();
        }

    }
}
