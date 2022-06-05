import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import zmq.Msg;

import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// Args [PUB-PORT] [REP-PORT] [ZONE] [PULL-PORT] [PUSH-PORT] [BOOT-PORT]
public class AggregatorServer {
    private static final HashMap<String,Integer> zoneToId = new HashMap<>();

    public static void main(String[] args) throws Exception{
        initMap();
        ZMQ.Context context = ZMQ.context(1);
        Aggregator ag = new Aggregator(args[1],zoneToId.get(args[1]));

        /*
        /* ------------ Inicialization ------------ */
        ZMQ.Socket bs = context.socket(SocketType.REQ);
        bs.connect("tcp://localhost:" + args[5]);

        bs.sendMore("NODE_ID".getBytes(ZMQ.CHARSET));
        bs.send("NODE_ID".getBytes(ZMQ.CHARSET));

        // Pode ser colocado num while com bs.hasReceiveMore()
        String neighbour = new String(bs.recv(),ZMQ.CHARSET);
        String port = new String(bs.recv(),ZMQ.CHARSET);
        System.out.println("Connection id -> \t" + neighbour + ":" + port);

        /* ---------------------------------------- */

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

        AggregatorNetwork network = new AggregatorNetwork(pll,ag);
        //AggregatorQueries quer = new AggregatorQueries(rep,ag);

        //System.out.println("A publicar na porta:\t" + args[0]);
        //System.out.println("A responder a queries porta:\t" + args[1]);



        ZMsg msg = new ZMsg();
        msg.add("A");
        msg.add(new byte[100000]);
        msg.send(psh);
        //psh.send(pushMessage.getBytes(ZMQ.CHARSET));    // IMPORTANT (CONTENT)

        new Thread(network).start();

        new Thread(() -> {
            while(true){
                ag.propagateState();
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        AggregatorNotifier notif = new AggregatorNotifier(pubPublic, ag);
        AggregatorQueries quer = new AggregatorQueries(rep, ag);
        AggregatorReceiver pusher = new AggregatorReceiver(psh, pll, ag);

        System.out.println("Publishing Port:\t" + args[0]);
        System.out.println("Reply Port:\t\t\t" + args[1]);
        System.out.println("Pull Port:\t\t\t" + args[3]);
        System.out.println("Push Port:\t\t\t" + args[4]);


        //new Thread(notif).start();
        //new Thread(quer).start();
        new Thread(pusher).start();
    }

    private static void initMap(){
        zoneToId.put("A",1);
        zoneToId.put("B",2);
        zoneToId.put("C",3);
        zoneToId.put("D",4);
        zoneToId.put("E",5);
    }
}
