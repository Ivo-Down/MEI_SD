import DataStructs.Table;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Aggregator {
    private final int id;

    private final Table neighbors;

    // Key -> AggregatorID. Value -> Information of the devices of that aggregator.
    //private Map<Integer, Map<String, DeviceTypeInformation>> struct;

    // Map that saves the nr of events of each type. Key -> Type;
    //private Map<String, Integer> eventsCounter;
    private final StateCRDT stateInfo;

    private HashMap<Integer, ZMQ.Socket> vizinhos; // id -> socketPush


<<<<<<< Updated upstream
    public Aggregator(int id, Table neighbors){
=======
    public Aggregator(int id){
>>>>>>> Stashed changes
        this.id = id;
        this.neighbors = neighbors;
        this.stateInfo = new StateCRDT();
        this.vizinhos = new HashMap<>();
    }

<<<<<<< Updated upstream

=======
>>>>>>> Stashed changes
    public int getId(){
        return this.id;
    }


    /* - - - - - - - FUNÇÕES AUXILIARES - - - - - - - - */


    // Função que envia o estado aos vizinhos TODO: Confirmar este send, por causa da identificação do envio do estado.
    public void propagateState(){
        for(ZMQ.Socket pushSocket: this.vizinhos.values()){
            ZMsg msg = new ZMsg();
            msg.add("A");
            msg.add(stateInfo.serialize());
            msg.send(pushSocket);
        }
    }

    public void receiveState(byte[] data){
        StateCRDT received = (StateCRDT) StateCRDT.deserialize(data);
        this.stateInfo.merge(received);
    }


    public boolean merge(StateCRDT newState){
        return this.stateInfo.merge(newState);
    }


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

    public void addEvents(List<String> eventsList){
        this.stateInfo.addEvents(eventsList, this.id);
    }

    public void updateDeviceState(Integer deviceId, String deviceState, String deviceType){
        this.stateInfo.updateDeviceState(deviceId, deviceState, deviceType, this.id);
    }
    public String toString() {
        return "Agregator{" +
                "id=" + id +
                ", neighbors=" + neighbors.toString() +
                '}';
    }
}
