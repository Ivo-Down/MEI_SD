import com.ericsson.otp.erlang.*;
import org.zeromq.ZMsg;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Task implements Runnable{
    private Aggregator aggregator;
    private AggregatorNotifier aggregatorNotifier;
    private String aux;
    private ZMsg msg;
    private Integer count;

    public Task(String aux, Aggregator aggregator, AggregatorNotifier aggregatorNotifier, ZMsg msg, Integer count){
        this.aggregator = aggregator;
        this.aux = aux;
        this.aggregatorNotifier = aggregatorNotifier;
        this.msg = msg;
        this.count = count;
    }
    @Override
    public void run() {
        try {
            if (aux.equals("A")) {  // Received info from an aggregator
                System.out.println("Estado de agregador recebida.");
                StateCRDT state = (StateCRDT) StaticMethods.deserialize(msg.pop().getData());

                if (this.aggregator.merge(state)) {
                    //this.aggregator.propagateState();
                    this.aggregatorNotifier.sendNotifications(state);
                }

            } else if (aux.equals("C_Device")) { // Received info from a colector about device's state

                OtpErlangMap deviceInfo = new OtpErlangMap(new OtpInputStream(msg.pop().getData()));
                //System.out.println("Conteudo da msg recebida:\t" + deviceInfo.toString());

                Integer deviceId = ((OtpErlangLong) deviceInfo.get(new OtpErlangAtom("id"))).intValue();

                count++;

                //System.out.println(count + " " + deviceId);

                Boolean deviceState = ((OtpErlangAtom) deviceInfo.get(new OtpErlangAtom("online"))).booleanValue();
                String deviceType = ((OtpErlangAtom) deviceInfo.get(new OtpErlangAtom("type"))).atomValue();

                this.aggregator.updateDeviceState(deviceId, deviceState, deviceType);
                //if (this.aggregator.updateDeviceState(deviceId, deviceState, deviceType))
                    //this.aggregator.propagateState();



            } else if (aux.equals("C_Event")) {  // Received info from a colector about device's state

                OtpErlangMap deviceInfo = new OtpErlangMap(new OtpInputStream(msg.pop().getData()));
                System.out.println("Conteudo da msg recebida:\t" + deviceInfo.toString());
                OtpErlangList eventsList = ((OtpErlangList) deviceInfo.get(new OtpErlangAtom("eventsList")));
                List<OtpErlangObject> erlObjects = Arrays.stream(eventsList.elements()).sequential().collect(Collectors.toList());

                this.aggregator.addEvents(erlObjects);
            }
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}
