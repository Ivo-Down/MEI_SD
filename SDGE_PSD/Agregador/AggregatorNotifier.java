import org.zeromq.ZMQ;

import java.util.Map;

public class AggregatorNotifier {
    protected ZMQ.Socket pubSocket;
    private StateCRDT cachedState;
    private final Integer aggId;

    public AggregatorNotifier(ZMQ.Socket pubSocket, Integer aggId) {
        this.pubSocket = pubSocket;
        this.cachedState = new StateCRDT(aggId);
        this.aggId = aggId;
    }

    public void sendNotifications(StateCRDT newState){
        checkPercentageChange(newState);
        checkOnlineRecord(newState);
        checkNewMissingType(newState);
        cachedState = new StateCRDT(newState);
    }

    // Checks if any type no longer has any online devices
    private void checkNewMissingType(StateCRDT newState) {
        if(cachedState == null) return;
        var onlineTypesOld = cachedState.checkTypesWithOnlineDevices(aggId);
        var onlineTypesNew = newState.checkTypesWithOnlineDevices(aggId);
        var missingTypes = onlineTypesOld.stream().filter( t -> !onlineTypesNew.contains(t)).toList();
        for (var mType : missingTypes) {
            pubSocket.sendMore("TOPIC_TYPE_GONE");
            pubSocket.send("Dispositivos do tipo " + mType + " deixaram de estar online!");
        }
    }

    // Checks if a new record has been set for this zone
    private void checkOnlineRecord(StateCRDT state) {
        if(cachedState == null) return;
        var oldOnline = cachedState.getRecords(aggId);
        var newOnline = state.getRecords(aggId);

        for(Map.Entry<String, Integer> entry: oldOnline.entrySet()){
            Integer newValue = newOnline.get(entry.getKey());
            if(entry.getValue() < newValue){
                pubSocket.sendMore("TOPIC_NEW_RECORD");
                pubSocket.send("Número recorde de " + entry.getKey() + " - " + newValue);
            }
        }

    }

    // Checks if the percentage of active devices in this zone has changed over a threshold
    private void checkPercentageChange(StateCRDT state) {
        if(cachedState == null) return;
        int oldPercentage = (cachedState.getOnlinePercentage(aggId)) / 10;
        int newPercentage = (state.getOnlinePercentage(aggId)) / 10;

        if(newPercentage > oldPercentage){
            pubSocket.sendMore("TOPIC_DEVICES_INCREASE");
            pubSocket.send("Percentagem de dispositivos online aumentou para " + (newPercentage*10) + "%");
        } else if(newPercentage < oldPercentage){
            pubSocket.sendMore("TOPIC_DEVICES_DECREASE");
            pubSocket.send("Percentagem de dispositivos online diminuiu para " + (newPercentage*10) + "%");
        }
    }
}
