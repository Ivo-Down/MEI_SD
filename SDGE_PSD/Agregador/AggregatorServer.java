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

        //ID
        Aggregator ag = handleNeighbors(Integer.parseInt(args[0]));
        System.out.println(ag);

        // ZeroMQ para PUBLISHER
        ZMQ.Socket pub = context.socket(SocketType.PUB);
        pub.connect("tcp://localhost:" + args[1]); // connect to broker

        // ZeroMQ para REPLY
        ZMQ.Socket rep = context.socket(SocketType.REP);
        rep.bind("tcp://localhost:" + args[2]);

        // ZeroMQ para PULL
        ZMQ.Socket pull = context.socket(SocketType.PULL);
        pull.bind("tcp://localhost:" + args[3]);

        // ZeroMQ para PUSH
        ZMQ.Socket push = context.socket(SocketType.PUSH);
        push.connect("tcp://localhost:" + args[4]);

        // Receives and sends requests from collectors and other aggregators
        AggregatorNetwork network = new AggregatorNetwork(pull, push, pub, ag);
        // Allows users to query the state
        AggregatorQueries quer = new AggregatorQueries(rep, ag);

        /*ZMsg msg = new ZMsg();
        msg.add("A");
        msg.add(new byte[100000]);
        msg.send(push);*/
        //psh.send(pushMessage.getBytes(ZMQ.CHARSET));    // IMPORTANT (CONTENT)

        new Thread(network).start();
        //new Thread(notif).start();    //TODO DESCOMENTAR
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

    private static Aggregator handleNeighbors(Integer id) {
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
        Table neighbors = (Table) StaticMethods.deserialize(data);

        return new Aggregator(id, neighbors);

    }

    }
