import DataStructs.Table;
import com.ericsson.otp.erlang.OtpErlangObject;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Aggregator {
    private final int id;

    private final StateCRDT stateInfo;

    private HashMap<Integer, ZMQ.Socket> vizinhos; // id -> socketPush


    public Aggregator(int id, Table neighbours, ZMQ.Context context){
        this.id = id;
        this.stateInfo = new StateCRDT(id);
        this.vizinhos = new HashMap<>();

        System.out.println(neighbours.toString());

        for (Map.Entry<Integer, Integer> vizinho : neighbours.getMap().entrySet()){
            ZMQ.Socket push = context.socket(SocketType.PUSH);
            push.connect("tcp://localhost:" + vizinho.getKey());
            this.vizinhos.put(vizinho.getValue(), push);
        }
    }


    /* - - - - - - - FUNÇÕES AUXILIARES - - - - - - - - */


    // Função que envia o estado aos vizinhos TODO: Confirmar este send, por causa da identificação do envio do estado.
    public void propagateState(){
        for(ZMQ.Socket pushSocket: this.vizinhos.values()){
            System.out.println("Propagating to " + pushSocket);
            ZMsg msg = new ZMsg();
            msg.add("A");
            msg.add(stateInfo.serialize());
            msg.send(pushSocket);
        }
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

    public void addEvents(List<OtpErlangObject> eventsList){
        this.stateInfo.addEvents(eventsList, this.id);
    }

    public boolean updateDeviceState(Integer deviceId, Boolean deviceState, String deviceType){
        return this.stateInfo.updateDeviceState(deviceId, deviceState, deviceType, this.id);
    }

    /*
    public String toString() {
        return "Aggregator{" +
                "id=" + id +
                ", neighbors=" + neighbours.toString() +
                '}';
    }
     */
}
