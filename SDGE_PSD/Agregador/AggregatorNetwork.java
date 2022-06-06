import com.ericsson.otp.erlang.OtpErlangMap;
import com.ericsson.otp.erlang.OtpInputStream;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.ArrayList;

// This module will receive updates from other aggregators, connecting to its neighbours, as well as collectors
public class AggregatorNetwork implements Runnable{
    private final ZMQ.Socket pull;
    private final ZMQ.Socket push;
    private final Aggregator aggregator;

    //TODO: pensar em estruturas para guardar informações sobre notis. (Um id por tipo - 4 tipos diferentes (?))
    // Mudar os nomes destas cenas.

    public AggregatorNetwork(ZMQ.Socket pull, ZMQ.Socket push, Aggregator aggregator) throws Exception {
        this.pull = pull;
        this.push = push;
        this.aggregator = aggregator;
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
                    }

                } else if (aux.equals("C")) {
                    // Receber +1 frame que identifica o tipo de not.

                    OtpErlangMap deviceInfo = new OtpErlangMap(new OtpInputStream(msg.pop().getData()));
                    System.out.println("Conteudo da msg recebida:\t" + deviceInfo.toString());

                    // TODO: Testar se o deserialize abaixo funciona
                    Integer deviceId = (Integer) StateCRDT.deserialize(msg.pop().getData());
                    String deviceState = (String) StateCRDT.deserialize(msg.pop().getData());
                    String deviceType = (String) StateCRDT.deserialize(msg.pop().getData());
                    this.aggregator.updateDeviceState(deviceId, deviceState, deviceType);
                    this.aggregator.propagateState();

                    // TODO: Testar se o deserialize abaixo funciona
                    ArrayList<String> eventsList = (ArrayList<String>) StateCRDT.deserialize(msg.pop().getData());
                    this.aggregator.addEvents(eventsList);
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