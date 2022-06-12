import com.ericsson.otp.erlang.*;
import org.zeromq.ZMsg;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Task implements Runnable{
    private final Aggregator aggregator;
    private final AggregatorNotifier aggregatorNotifier;
    private final String aux;
    private final ZMsg msg;

    public Task(String aux, Aggregator aggregator, AggregatorNotifier aggregatorNotifier, ZMsg msg){
        this.aggregator = aggregator;
        this.aux = aux;
        this.aggregatorNotifier = aggregatorNotifier;
        this.msg = msg;
    }
    @Override
    public void run() {
        try {
            if (aux.equals("A")) {  // Received info from an aggregator
                //System.out.println("AGG" + aggregator.getId() + " - Received state");
                StateCRDT state = (StateCRDT) StaticMethods.deserialize(msg.pop().getData());

                if (this.aggregator.merge(state)) {
                    this.aggregatorNotifier.sendNotifications(this.aggregator.getState());
                }

            } else if (aux.equals("C_Device")) { // Received device's state from collector
                OtpErlangMap deviceInfo = new OtpErlangMap(new OtpInputStream(msg.pop().getData()));

                System.out.println("AGG"+ aggregator.getId() +" - Device state receives:\t" + deviceInfo);

                Integer deviceId = ((OtpErlangLong) deviceInfo.get(new OtpErlangAtom("id"))).intValue();

                Boolean deviceState = ((OtpErlangAtom) deviceInfo.get(new OtpErlangAtom("online"))).booleanValue();
                String deviceType = ((OtpErlangAtom) deviceInfo.get(new OtpErlangAtom("type"))).atomValue();

                this.aggregator.updateDeviceState(deviceId, deviceState, deviceType);
                this.aggregatorNotifier.sendNotifications(this.aggregator.getState());

            } else if (aux.equals("C_Event")) {  // Received events from collector

                OtpErlangMap deviceInfo = new OtpErlangMap(new OtpInputStream(msg.pop().getData()));
                System.out.println("AGG"+ aggregator.getId() +" - Events received:\t" + deviceInfo);
                OtpErlangList eventsList = ((OtpErlangList) deviceInfo.get(new OtpErlangAtom("eventsList")));
                List<OtpErlangObject> erlObjects = Arrays.stream(eventsList.elements()).sequential().collect(Collectors.toList());

                this.aggregator.addEvents(erlObjects);
            }
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}
