import org.zeromq.SocketType;
import org.zeromq.ZMQ;

import java.util.HashMap;

// Args [PUB-PORT] [REP-PORT] [ZONE] [PULL-PORT] [PUSH-PORT] [BOOT-PORT]
public class AggregatorServer {
    private static final HashMap<String,Integer> zoneToId = new HashMap<>();

    public static void main(String[] args) throws Exception{
        initMap();
        ZMQ.Context context = ZMQ.context(1);
        Aggregator ag = new Aggregator(args[2],zoneToId.get(args[2]));

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
        pll.connect("tcp://localhost:" + args[3]);

        // ZeroMQ para PULL
        ZMQ.Socket psh = context.socket(SocketType.PUSH);
        psh.bind("tcp://localhost:" + args[4]);


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
