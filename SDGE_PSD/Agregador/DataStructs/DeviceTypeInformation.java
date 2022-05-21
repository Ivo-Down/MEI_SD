package DataStructs;

import java.util.HashMap;
import java.util.Map;

public class DeviceTypeInformation {

    private Integer onlineRecord;
    private Integer onlineCounter;

    private Map<Integer, DeviceInformation> devices;

    public DeviceTypeInformation() {
        this.onlineCounter = 0;
        this.onlineRecord = 0;
        this.devices = new HashMap<>();
    }
}
