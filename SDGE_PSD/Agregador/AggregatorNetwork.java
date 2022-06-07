import DataStructs.NotificationData;
import DataStructs.StateCRDT;
import com.ericsson.otp.erlang.*;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Map;

// This module will receive updates from other aggregators, connecting to its neighbours, as well as collectors
public class AggregatorNetwork implements Runnable{
    private final ZMQ.Socket pull;
    private final ZMQ.Socket push;
    private final Aggregator aggregator;
    private final NotificationData nData;

    //TODO: pensar em estruturas para guardar informações sobre notis. (Um id por tipo - 4 tipos diferentes (?))
    // Mudar os nomes destas cenas.

    public AggregatorNetwork(ZMQ.Socket pull, ZMQ.Socket push, ZMQ.Socket pub, Aggregator aggregator) throws Exception {
        this.pull = pull;
        this.push = push;
        this.aggregator = aggregator;
        this.nData = new NotificationData(pub);
    }

    public void run(){

        while(true){
           //Aqui uma função de receber e dar parse da mensagem
            try{

                ZMsg msg = ZMsg.recvMsg(this.pull);
                System.out.println("Pulled request:\t" + msg.toString());

                String aux = new String(msg.pop().getData(),ZMQ.CHARSET);

                System.out.println(aux);

                if(aux.equals("A")){
                    System.out.println("Estado de agregador recebida.");
                    StateCRDT state = (StateCRDT) StateCRDT.deserialize(msg.pop().getData());

                    if (this.aggregator.merge(state)){
                        this.aggregator.propagateState();
                        //this.nData.sendNotifications(state);    TODO DESCOMENTAR
                    }

                }
                else if (aux.equals("C_Device")) {
                    // Receber +1 frame que identifica o tipo de not.

                    OtpErlangMap deviceInfo = new OtpErlangMap(new OtpInputStream(msg.pop().getData()));
                    System.out.println("Conteudo da msg recebida:\t" + deviceInfo.toString());

                    Integer deviceId = ((OtpErlangLong) deviceInfo.get(new OtpErlangAtom("id"))).intValue();
                    Boolean deviceState = ((OtpErlangAtom) deviceInfo.get(new OtpErlangAtom("online"))).booleanValue();
                    String deviceType = ((OtpErlangAtom) deviceInfo.get(new OtpErlangAtom("type"))).atomValue();

                    this.aggregator.updateDeviceState(deviceId, deviceState, deviceType);
                    this.aggregator.propagateState();


                }

                else if(aux.equals("C_Event")){

                    OtpErlangMap deviceInfo = new OtpErlangMap(new OtpInputStream(msg.pop().getData()));
                    System.out.println("Conteudo da msg recebida:\t" + deviceInfo.toString());
                    OtpErlangList eventsList = ((OtpErlangList) deviceInfo.get(new OtpErlangAtom("eventsList")));
                    //ArrayList<String> eventsList = (ArrayList<String>) DataStructs.StateCRDT.deserialize(msg.pop().getData());
                    //this.aggregator.addEvents(eventsList);
                }

                //System.out.println("Received an update: " + response);
                Thread.sleep(1000);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }



}