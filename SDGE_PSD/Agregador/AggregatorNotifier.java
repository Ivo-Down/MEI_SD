import org.zeromq.ZMQ;

import java.util.stream.Collectors;

public class AggregatorNotifier {
    protected ZMQ.Socket pushSocket;
    private StateCRDT cachedState;

    //* IDEAS *//
    // - Use multiple fields to check previous values
    // - Instead of adding multiple fields, save a deep clone of the state and use streams to compare state

    public AggregatorNotifier(ZMQ.Socket pushSocket) {

        this.pushSocket = pushSocket;
        this.cachedState = new StateCRDT();
    }

    public void sendNotifications(StateCRDT newState){
        checkOnlineDevices(newState);
        checkRecordDevices(newState);
        checkOnlineIncrease(newState);
        CheckOnlineDecrease(newState);
        cachedState = newState;
    }

    // checkar quando deixar de haver dispositivos online de um dado tipo na zona;
    private void CheckOnlineDecrease(StateCRDT newstate) {
        if(cachedState == null) return;
        var onlineTypesOld = cachedState.checkTypesWithOnlineDevices();
        var onlineTypesNew = newstate.checkTypesWithOnlineDevices();
        var missingTypes = onlineTypesOld.stream().filter(onlineTypesNew::contains).toList();
        if(missingTypes.size() == 0) return;
        for (var mType : missingTypes) {
            pushSocket.sendMore("TOPIC_TYPE_GONE");
            pushSocket.send(mType + " deixaram de estar online!");
        }
    }
    // checkar atingido record de número de dispositivos online de um dado tipo na zona (com informação do
    // seu valor); um cliente poderá estar interessado em saber se foi atingido algum record, para algum
    // tipo de dispositivo (e não um tipo em particular)
    private void checkOnlineIncrease(StateCRDT state) {
        if(cachedState == null) return;
        var oldOnline = cachedState.getDevicesOnline();
        var newOnline = state.getDevicesOnline();
        if(newOnline - oldOnline <= 0) return;
        pushSocket.sendMore("TOPIC_ONLINE_INCREASE");
        pushSocket.send("Número de dispositivos online aumentou em " + (newOnline - oldOnline));
    }
    // a percentagem de dispositivos online na zona face ao total online subiu, tendo ficado em mais de
    // X% dos dispositivos online, para X ∈ {10, 20, . . . , 90}
    private void checkRecordDevices(StateCRDT state) {
        if(cachedState == null) return;
        int oldPercentage = (int)(cachedState.getOnlinePercentage()*100) % 10;
        int newPercentage = (int)(state.getOnlinePercentage()*100) % 10;
        if(newPercentage > oldPercentage){
            pushSocket.sendMore("TOPIC_DEVICES_INCREASE");
            pushSocket.send("Percentagem de dispositivos online aumentou " + (newPercentage*10));
        }
    }
    // a percentagem de dispositivos online na zona face ao total online desceu, tendo ficado em menos
    // de X% dos dispositivos online, para X ∈ {10, 20, . . . , 90}
    private void checkOnlineDevices(StateCRDT state) {
        if(cachedState == null) return;
        int oldPercentage = (int)(cachedState.getOnlinePercentage()*100) % 10;
        int newPercentage = (int)(state.getOnlinePercentage()*100) % 10;
        if(newPercentage < oldPercentage){
            pushSocket.sendMore("TOPIC_DEVICES_DECREASE");
            pushSocket.send("Percentagem de dispositivos online diminuiu " + (newPercentage*10));
        }
    }
}
