import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import zmq.Msg;

import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// Args [PUB-PORT] [REP-PORT] [ZONE] [PULL-PORT] [PUSH-PORT]
public class AggregatorServer {
    private static final HashMap<String,Integer> zoneToId = new HashMap<>();

    public static void main(String[] args) throws Exception{
        initMap();
        ZMQ.Context context = ZMQ.context(1);
        Aggregator ag = new Aggregator(args[1],zoneToId.get(args[2]));

        // ZeroMQ para PUBLISHER
        ZMQ.Socket pubPublic = context.socket(SocketType.PUB);
        pubPublic.connect("tcp://localhost:" + args[0]); // connect to broker

        // ZeroMQ para REPLY
        ZMQ.Socket rep = context.socket(SocketType.REP);
        rep.bind("tcp://localhost:" + args[1]);

        // ZeroMQ para PUSH
        ZMQ.Socket pll = context.socket(SocketType.PULL);
        pll.bind("tcp://localhost:8888");

        // ZeroMQ para PULL
        ZMQ.Socket psh = context.socket(SocketType.PUSH);
        psh.connect("tcp://localhost:8888");

        // Receives and sends requests from collectors and other aggregators
        AggregatorNetwork network = new AggregatorNetwork(pll,psh,ag);
        // Notifies users about specific state changes
        AggregatorNotifier notif = new AggregatorNotifier(pubPublic, ag);
        // Allows users to query the state
        //AggregatorQueries quer = new AggregatorQueries(rep, ag);

        ZMsg msg = new ZMsg();
        msg.add("A");
        msg.add(new byte[100000]);
        msg.send(psh);
        //psh.send(pushMessage.getBytes(ZMQ.CHARSET));    // IMPORTANT (CONTENT)

        new Thread(network).start();
        new Thread(notif).start();
        //new Thread(quer).start();

        // Propagate state, TODO: maybe try with new class
        new Thread(() -> {
            while(!Thread.currentThread().isInterrupted()){
                ag.propagateState();
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
    }

    private static void initMap(){
        zoneToId.put("A",1);
        zoneToId.put("B",2);
        zoneToId.put("C",3);
        zoneToId.put("D",4);
        zoneToId.put("E",5);
    }
}
