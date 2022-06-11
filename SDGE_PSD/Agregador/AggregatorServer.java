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


// Args [ID-AGG] [PUB-PORT] [REP-PORT] [PULL-PORT] [PUSH-PORT]
public class AggregatorServer {
    private static final ZMQ.Context context = ZMQ.context(1);
    public static final int bootstrapper_port = 8888;


    public static void main(String[] args) throws Exception{

        Integer id = Integer.parseInt(args[0]);

        if(id > 100){
            System.out.println("Invalid ID (must be below 100)");
            return;
        }
        //ID
        Aggregator ag = handleNeighbours(id);
        System.out.println(ag);

        Integer pubPort = 8100 + id;
        Integer repPort = 8200 + id;
        Integer pullPort = 8300 + id;
        Integer pushPort = 8400 + id;

        System.out.println("pubPort: " + pubPort + " repPort: " + repPort + " pullPort: " + pullPort + " pushPort: " + pushPort);

        // ZeroMQ para PUBLISHER
        ZMQ.Socket pub = context.socket(SocketType.PUB);
        pub.bind("tcp://localhost:" + pubPort); // connect to broker

        // ZeroMQ para REPLY
        ZMQ.Socket rep = context.socket(SocketType.REP);
        rep.bind("tcp://localhost:" + repPort);

        // ZeroMQ para PULL
        ZMQ.Socket pull = context.socket(SocketType.PULL);
        pull.setBacklog(2000);
        pull.bind("tcp://localhost:" + pullPort);

        // ZeroMQ para PUSH
        ZMQ.Socket push = context.socket(SocketType.PUSH);
        push.connect("tcp://localhost:" + pushPort);

        // Receives and sends requests from collectors and other aggregators
        AggregatorNetwork network = new AggregatorNetwork(pull, push, pub, ag);
        // Allows users to query the state
        AggregatorQueries quer = new AggregatorQueries(rep, ag);


        new Thread(network).start();
        new Thread(quer).start();

        // Propagate state, TODO: maybe try with new class
        new Thread(() -> {
            while(!Thread.currentThread().isInterrupted()){
                ag.propagateState();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        /*
        while(true){
            //Aqui uma função de receber e dar parse da mensagem
            try{
                ZMsg msg = ZMsg.recvMsg(pull);
                OtpErlangMap request = new OtpErlangMap(new OtpInputStream(msg.pop().getData()));
                System.out.println("Pulled request:\t" + request.toString());
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }*/

    }

    private static Aggregator handleNeighbours(Integer id) {
        /* ------------ Initialization ------------ */
        ZMQ.Socket reqNeighbours = context.socket(SocketType.REQ);
        reqNeighbours.connect("tcp://localhost:" + bootstrapper_port);

        // Envia o Pedido de vizinhos
        String s = Integer.toString(id);
        reqNeighbours.sendMore("Quero os meus vizinhos...".getBytes(ZMQ.CHARSET));
        reqNeighbours.send(s.getBytes(ZMQ.CHARSET)); //manda ID

        // Recebe a resposta do Bootstrapper
        String resposta = new String(reqNeighbours.recv(),ZMQ.CHARSET); // Vem a mensagem introdutória
        byte[] data = reqNeighbours.recv(); //Vem a tabela
        System.out.println("->Received neighbors information from bootstrapper.\n");
        Table neighbours = (Table) StaticMethods.deserialize(data);

        return new Aggregator(id, neighbours, context);

    }

    }
