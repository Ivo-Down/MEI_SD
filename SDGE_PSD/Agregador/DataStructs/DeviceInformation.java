package DataStructs;

import java.util.ArrayList;
import java.util.List;

@Deprecated
public class DeviceInformation {
    private Boolean online; //True if On, False if Off
    private List<String> events;

    private DeviceInformation(){
        this.events = new ArrayList<>();
    }
    public boolean isOnline(){
        return online;
    }
}

