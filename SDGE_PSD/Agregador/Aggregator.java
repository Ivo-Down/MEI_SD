import DataStructs.DeviceTypeInformation;
import DataStructs.ZoneInformation;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.HashMap;
import java.util.Map;

public class Aggregator {
    private final String zoneName;
    private final int id;

    private StateCRDT stateInfo;

    private HashMap<Integer, ZMQ.Socket> vizinhos; // id -> socketPush


    public Aggregator(String zoneName, int id){
        this.zoneName = zoneName;
        this.id = id;
        this.stateInfo = new StateCRDT();
        this.vizinhos = new HashMap<>();
    }


    public boolean merge(StateCRDT newState){
        return this.stateInfo.merge(newState);
    }
    public String getzoneName(){
        return zoneName;
    }

    public int getId(){
        return id;
    }

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
