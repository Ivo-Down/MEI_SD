import org.zeromq.ZMQ;

import java.util.Map;

public class AggregatorNotifier {
    protected ZMQ.Socket pushSocket;
    private StateCRDT cachedState;
    private Integer aggId;

    //* IDEAS *//
    // - Use multiple fields to check previous values
    // - Instead of adding multiple fields, save a deep clone of the state and use streams to compare state

    public AggregatorNotifier(ZMQ.Socket pushSocket, Integer aggId) {

        this.pushSocket = pushSocket;
        this.cachedState = new StateCRDT(aggId);
        this.aggId = aggId;
    }

    public void sendNotifications(StateCRDT newState){
        checkPercentageChange(newState);
        checkOnlineRecord(newState);
        checkNewMissingType(newState);
        cachedState = new StateCRDT(newState);
    }

    // checkar quando deixar de haver dispositivos online de um dado tipo na zona;
    private void checkNewMissingType(StateCRDT newstate) {
        if(cachedState == null) return;
        var onlineTypesOld = cachedState.checkTypesWithOnlineDevices(aggId);
        var onlineTypesNew = newstate.checkTypesWithOnlineDevices(aggId);
        var missingTypes = onlineTypesOld.stream().filter( t -> !onlineTypesNew.contains(t)).toList();
        System.out.println("Missing " + missingTypes.toString());
        for (var mType : missingTypes) {
            pushSocket.sendMore("TOPIC_TYPE_GONE");
            pushSocket.send("Dispositivos do tipo " + mType + " deixaram de estar online!");
        }
    }
    // checkar atingido record de número de dispositivos online de um dado tipo na zona (com informação do
    // seu valor); um cliente poderá estar interessado em saber se foi atingido algum record, para algum
    // tipo de dispositivo (e não um tipo em particular)
    private void checkOnlineRecord(StateCRDT state) {
        if(cachedState == null) return;
        var oldOnline = cachedState.getRecords(aggId);
        var newOnline = state.getRecords(aggId);

        for(Map.Entry<String, Integer> entry: oldOnline.entrySet()){
            Integer newValue = newOnline.get(entry.getKey());
            if(entry.getValue() < newValue){
                pushSocket.sendMore("TOPIC_NEW_RECORD");
                pushSocket.send("Número recorde de " + entry.getKey() + " - " + newValue);
            }
        }

    }
    // a percentagem de dispositivos online na zona face ao total online subiu, tendo ficado em mais de
    // X% dos dispositivos online, para X ∈ {10, 20, . . . , 90}
    private void checkPercentageChange(StateCRDT state) {
        if(cachedState == null) return;
        int oldPercentage = (cachedState.getOnlinePercentage(aggId)) / 10;
        int newPercentage = (state.getOnlinePercentage(aggId)) / 10;

        System.out.println(" old " + oldPercentage + " new " +newPercentage);

        if(newPercentage > oldPercentage){
            //System.out.println("Percentagem de dispositivos online aumentou para " + (newPercentage*10) + "%");
            pushSocket.sendMore("TOPIC_DEVICES_INCREASE");
            pushSocket.send("Percentagem de dispositivos online aumentou para " + (newPercentage*10) + "%");
        } else if(newPercentage < oldPercentage){
            //System.out.println("Percentagem de dispositivos online diminuiu para " + (newPercentage*10) + "%");
            pushSocket.sendMore("TOPIC_DEVICES_DECREASE");
            pushSocket.send("Percentagem de dispositivos online diminuiu para " + (newPercentage*10) + "%");
        }
    }
}
