import org.zeromq.ZMQ;

public class AggregatorNotifier {
    protected ZMQ.Socket pushSocket;
    private StateCRDT cachedState;

    //* IDEAS *//
    // - Use multiple fields to check previous values
    // - Instead of adding multiple fields, save a deep clone of the state and use streams to compare state

    public AggregatorNotifier(ZMQ.Socket pushSocket) {
        this.pushSocket = pushSocket;
    }

    public void sendNotifications(StateCRDT newState){
        checkOnlineDevices(newState);
        checkRecordDevices(newState);
        checkOnlineIncrease(newState);
        CheckOnlineDecrease(newState);
        cachedState = newState;
    }
    // TODO: TRADUZIR PARA INGLISH

    // checkar quando deixar de haver dispositivos online de um dado tipo na zona;
    private void CheckOnlineDecrease(StateCRDT state) {
        if(cachedState == null) return;
        //TODO: DO CHECKS
        pushSocket.sendMore("TOPIC");
        pushSocket.send("CONTENT");
    }
    // checkar atingido record de número de dispositivos online de um dado tipo na zona (com informação do
    // seu valor); um cliente poderá estar interessado em saber se foi atingido algum record, para algum
    // tipo de dispositivo (e não um tipo em particular)
    private void checkOnlineIncrease(StateCRDT state) {
        if(cachedState == null) return;
        //TODO: DO CHECKS
        pushSocket.sendMore("TOPIC");
        pushSocket.send("CONTENT");
    }
    // a percentagem de dispositivos online na zona face ao total online subiu, tendo ficado em mais de
    // X% dos dispositivos online, para X ∈ {10, 20, . . . , 90}
    private void checkRecordDevices(StateCRDT state) {
        if(cachedState == null) return;
        //TODO: DO CHECKS
        pushSocket.sendMore("TOPIC");
        pushSocket.send("CONTENT");
    }
    // a percentagem de dispositivos online na zona face ao total online desceu, tendo ficado em menos
    // de X% dos dispositivos online, para X ∈ {10, 20, . . . , 90}
    private void checkOnlineDevices(StateCRDT state) {
        if(cachedState == null) return;
        //TODO: DO CHECKS
        pushSocket.sendMore("TOPIC");
        pushSocket.send("CONTENT");
    }
}
