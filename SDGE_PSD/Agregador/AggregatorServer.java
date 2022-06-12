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

        int id = Integer.parseInt(args[0]);

        if(id > 100) {
            System.out.println("Invalid ID (must be below 100)");
            return;
        }
        Aggregator ag = handleNeighbours(id);

        int pubPort = 8100 + id;
        int repPort = 8200 + id;
        int pullPort = 8300 + id;

        System.out.println("pubPort: " + pubPort + " repPort: " + repPort + " pullPort: " + pullPort);

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

        // Receives and sends requests from collectors and other aggregators
        AggregatorNetwork network = new AggregatorNetwork(pull, pub, ag);
        // Allows users to query the state
        AggregatorQueries queries = new AggregatorQueries(rep, ag);

        new Thread(network).start();
        new Thread(queries).start();

        while(!Thread.currentThread().isInterrupted()){
            ag.propagateState();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private static Aggregator handleNeighbours(Integer id) {
        /* ------------ Initialization ------------ */
        ZMQ.Socket reqNeighbours = context.socket(SocketType.REQ);
        reqNeighbours.connect("tcp://localhost:" + bootstrapper_port);

        // Envia o Pedido de vizinhos
        String s = Integer.toString(id);
        reqNeighbours.send(s.getBytes(ZMQ.CHARSET));

        // Recebe a resposta do Bootstrapper
        byte[] data = reqNeighbours.recv();
        System.out.println("AGG - Received neighbors information from bootstrapper.\n");
        Table neighbours = (Table) StaticMethods.deserialize(data);

        return new Aggregator(id, neighbours, context);

    }
}
