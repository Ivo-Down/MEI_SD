import org.zeromq.SocketType;
import org.zeromq.ZMQ;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;


public class AgregatorServer {
    private static final HashMap<String,Integer> zoneToId = new HashMap<>();

    /**
     * District.DistrictServer class for the district server
     * @param args arg0 -> privateNotifications, arg1 -> broker, arg2 -> GetFrontendPings, arg3 -> atomName, arg4 -> directory port
     * Example: 12347 8001 8102 braga 8080
     */
    public static void main(String[] args) throws Exception{
        initMap();

        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket pubPublic = context.socket(SocketType.PUB);
        pubPublic.connect("tcp://localhost:" + args[0]); // connect to broker
        System.out.println("*** Agregador da ZONA " + args[1] + " está ativo ***");

        System.out.println("A conectar à porta..." + args[0]);

        Agregator d = new Agregator(args[1],zoneToId.get(args[1]));
        AgregNotifier notif = new AgregNotifier(pubPublic,d);
        new Thread(notif).start();


        //Aqui entra a parte do PUSH



        //Aqui entra a parte do REPLY
    }

    private static void initMap(){
        zoneToId.put("A",1);
        zoneToId.put("B",2);
        zoneToId.put("C",3);
        zoneToId.put("D",4);
        zoneToId.put("E",5);
    }
}
