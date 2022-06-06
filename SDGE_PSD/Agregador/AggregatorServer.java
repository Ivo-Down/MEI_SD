import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import zmq.Msg;
import DataStructs.Table;

import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// Args [ID-AGG] [PUB-PORT] [REP-PORT] [ZONE] [PULL-PORT] [PUSH-PORT]
public class AggregatorServer {
    private static final ZMQ.Context context = ZMQ.context(1);
    public static final int bootstrapper_port = 8888;


    public static void main(String[] args) throws Exception{

        //ID
        Aggregator ag = handleNeighbors(Integer.parseInt(args[0]));
        System.out.println(ag);

        // ZeroMQ para PUBLISHER
        ZMQ.Socket pubPublic = context.socket(SocketType.PUB);
        pubPublic.connect("tcp://localhost:" + args[1]); // connect to broker

        // ZeroMQ para REPLY
        ZMQ.Socket rep = context.socket(SocketType.REP);
        rep.bind("tcp://localhost:" + args[2]);

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

    private static Aggregator handleNeighbors(Integer id) {
        /* ------------ Inicialization ------------ */
        ZMQ.Socket reqneighbours = context.socket(SocketType.REQ);
        reqneighbours.connect("tcp://localhost:" + bootstrapper_port);

        // Envia o Pedido de vizinhos
        String s = Integer.toString(id);
        reqneighbours.sendMore("Quero os meus vizinhos...".getBytes(ZMQ.CHARSET));
        reqneighbours.send(s.getBytes(ZMQ.CHARSET)); //manda ID

        // Recebe a resposta do Bootstrapper
        String resposta = new String(reqneighbours.recv(),ZMQ.CHARSET); // Vem a mensagem introdutória
        byte[] data = reqneighbours.recv(); //Vem a tabela
        System.out.println("->Received neighbors information from bootstrapper.\n");
        Table neighbors = (Table) StaticMethods.deserialize(data);

        return new Aggregator(id, neighbors);

    }

    }
