import com.ericsson.otp.erlang.OtpErlangMap;
import com.ericsson.otp.erlang.OtpInputStream;
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
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
    private static final ZMQ.Context context = ZMQ.context(1);
    public static final int bootstrapper_port = 8888;


    public static void main(String[] args) throws Exception{
=======
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes

    public static void main(String[] args) throws Exception{
        ZMQ.Context context = ZMQ.context(1);
        Aggregator ag = new Aggregator(Integer.parseInt(args[1]));

        /*
        /* ------------ Inicialization ------------ */
        /*ZMQ.Socket bs = context.socket(SocketType.REQ);
        bs.connect("tcp://localhost:" + args[5]);

        bs.sendMore("NODE_ID".getBytes(ZMQ.CHARSET));
        bs.send("NODE_ID".getBytes(ZMQ.CHARSET));

        // Pode ser colocado num while com bs.hasReceiveMore()
        String neighbour = new String(bs.recv(),ZMQ.CHARSET);
        String port = new String(bs.recv(),ZMQ.CHARSET);
        System.out.println("Connection id -> \t" + neighbour + ":" + port); */
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes

        //ID
        Aggregator ag = handleNeighbors(Integer.parseInt(args[0]));
        System.out.println(ag);

        // ZeroMQ para PUBLISHER
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
        ZMQ.Socket pubPublic = context.socket(SocketType.PUB);
        pubPublic.connect("tcp://localhost:" + args[1]); // connect to broker

        // ZeroMQ para REPLY
        ZMQ.Socket rep = context.socket(SocketType.REP);
        rep.bind("tcp://localhost:" + args[2]);
=======
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======

        /* ---------------------------------------- */

        // ZeroMQ para PUBLISHER
>>>>>>> Stashed changes
       /* ZMQ.Socket pubPublic = context.socket(SocketType.PUB);
        pubPublic.connect("tcp://localhost:" + args[0]); // connect to broker

        // ZeroMQ para REPLY
        ZMQ.Socket rep = context.socket(SocketType.REP);
        rep.bind("tcp://localhost:" + args[1]); */
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes

        // ZeroMQ para PUSH
        ZMQ.Socket pll = context.socket(SocketType.PULL);
        pll.bind("tcp://localhost:8888");

        // ZeroMQ para PULL
        //ZMQ.Socket psh = context.socket(SocketType.PUSH);
        //psh.connect("tcp://localhost:8888");

<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
        // Receives and sends requests from collectors and other aggregators
        AggregatorNetwork network = new AggregatorNetwork(pll,psh,ag);
        // Notifies users about specific state changes
        AggregatorNotifier notif = new AggregatorNotifier(pubPublic, ag);
        // Allows users to query the state
        //AggregatorQueries quer = new AggregatorQueries(rep, ag);
=======
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
        //AggregatorNetwork network = new AggregatorNetwork(pll,ag);
        //AggregatorQueries quer = new AggregatorQueries(rep,ag);

        //System.out.println("A publicar na porta:\t" + args[0]);
        //System.out.println("A responder a queries porta:\t" + args[1]);


>>>>>>> Stashed changes

        ZMsg msg = new ZMsg();
        msg.add("A");
        msg.add(new byte[100000]);
        //msg.send(psh);
        //psh.send(pushMessage.getBytes(ZMQ.CHARSET));    // IMPORTANT (CONTENT)

<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
        new Thread(network).start();
        new Thread(notif).start();
        //new Thread(quer).start();

        // Propagate state, TODO: maybe try with new class
        new Thread(() -> {
            while(!Thread.currentThread().isInterrupted()){
=======
       // new Thread(network).start();

=======
       // new Thread(network).start();

>>>>>>> Stashed changes
=======
       // new Thread(network).start();

>>>>>>> Stashed changes
=======
       // new Thread(network).start();

>>>>>>> Stashed changes
       /* new Thread(() -> {
            while(true){
>>>>>>> Stashed changes
                ag.propagateState();
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
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
=======
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
        }).start(); */

        //AggregatorNotifier notif = new AggregatorNotifier(pubPublic, ag);
        //AggregatorQueries quer = new AggregatorQueries(rep, ag);
        //AggregatorReceiver pusher = new AggregatorReceiver(psh, pll, ag);
        while(true){
            //Aqui uma função de receber e dar parse da mensagem
            try{
                OtpErlangMap request = new OtpErlangMap(new OtpInputStream(pll.recv()));
                System.out.println("Pulled request:\t" + request.toString());
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
/*
        System.out.println("Publishing Port:\t" + args[0]);
        System.out.println("Reply Port:\t\t\t" + args[1]);
        System.out.println("Pull Port:\t\t\t" + args[3]);
        System.out.println("Push Port:\t\t\t" + args[4]); */
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes

        // Recebe a resposta do Bootstrapper
        String resposta = new String(reqneighbours.recv(),ZMQ.CHARSET); // Vem a mensagem introdutória
        byte[] data = reqneighbours.recv(); //Vem a tabela
        System.out.println("->Received neighbors information from bootstrapper.\n");
        Table neighbors = (Table) StaticMethods.deserialize(data);

<<<<<<< Updated upstream
        return new Aggregator(id, neighbors);

<<<<<<< Updated upstream
    }
=======
=======
>>>>>>> Stashed changes

>>>>>>> Stashed changes

=======
        //new Thread(notif).start();
        //new Thread(quer).start();
        //new Thread(pusher).start();
<<<<<<< Updated upstream
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
        //new Thread(notif).start();
        //new Thread(quer).start();
        //new Thread(pusher).start();
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
    }
