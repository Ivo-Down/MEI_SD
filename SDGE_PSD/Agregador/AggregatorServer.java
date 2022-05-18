import org.zeromq.SocketType;
import org.zeromq.ZMQ;

import java.util.HashMap;

//Args: portaPub portaRep zona
public class AggregatorServer {
    private static final HashMap<String,Integer> zoneToId = new HashMap<>();

    public static void main(String[] args) throws Exception{
        initMap();
        ZMQ.Context context = ZMQ.context(1);
        Aggregator ag = new Aggregator(args[2],zoneToId.get(args[2]));

        // ZeroMQ para PUBLISHER
        ZMQ.Socket pubPublic = context.socket(SocketType.PUB);
        pubPublic.connect("tcp://localhost:" + args[0]); // connect to broker

        // ZeroMQ para REPLY
        ZMQ.Socket rep = context.socket(SocketType.REP);
        rep.bind("tcp://localhost:" + args[1]);


        AggregatorNotifier notif = new AggregatorNotifier(pubPublic,ag);
        AggregatorQueries quer = new AggregatorQueries(rep,ag);

        System.out.println("A publicar na porta:\t" + args[0]);
        System.out.println("A responder a queries porta:\t" + args[1]);


        new Thread(notif).start();
        new Thread(quer).start();

        //Aqui entra a parte do PUSH




    }

    private static void initMap(){
        zoneToId.put("A",1);
        zoneToId.put("B",2);
        zoneToId.put("C",3);
        zoneToId.put("D",4);
        zoneToId.put("E",5);
    }
}
