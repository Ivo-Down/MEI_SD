import DataStructs.DeviceTypeInformation;
import DataStructs.ZoneInformation;

import java.util.HashMap;
import java.util.Map;

public class Aggregator {
    private final String zoneName;
    private final int id;

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



    /* - - - - - - - FUNÇÕES AUXILIARES - - - - - - - - */

    public boolean getIsDeviceOnline(int deviceId){
        return this.stateInfo.getIsDeviceOnline(deviceId);
    }

    public int getDevicesOnline(){
        return this.stateInfo.getDevicesOnline();
    }

    public int getDevicesOnlineOfType(String type){
        return this.stateInfo.getDevicesOnlineOfType(type);
    }

    public int getNumberEvents(String eventType){
        return this.stateInfo.getNumberEvents(eventType);
    }
}
