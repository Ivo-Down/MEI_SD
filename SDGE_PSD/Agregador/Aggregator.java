import DataStructs.DeviceTypeInformation;
import DataStructs.ZoneInformation;
import org.zeromq.ZMQ;

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
    }



    public String getzoneName(){
        return zoneName;
    }

    public int getId(){
        return id;
    }

    // Função que envia o estado aos vizinhos TODO: Confirmar este send, por causa da identificação do envio do estado.
    public void propagateState(){
        for(ZMQ.Socket pullSocket: this.vizinhos.values()){
            pullSocket.send(stateInfo.serialize());
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
