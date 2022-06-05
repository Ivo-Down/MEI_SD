import org.zeromq.SocketType;
import org.zeromq.ZMQ;

//Args: [SUB-PORT] [REQ-PORT]
public class Client {
    public static void main(String[] args) throws Exception{
        ZMQ.Context context = ZMQ.context(1);

        // ZeroMQ Socket para SUBSCRIBER
        ZMQ.Socket sub = context.socket(SocketType.SUB);
        sub.connect("tcp://localhost:" + args[0]);
        sub.subscribe("TOPIC_1");   // IMPORTANT (TOPIC)

        // ZeroMQ Socket para REQUEST
        ZMQ.Socket req = context.socket(SocketType.REQ);
        req.connect("tcp://localhost:" + args[1]);

        ClientNotifier cr = new ClientNotifier(sub);
        ClientQueries cq = new ClientQueries(req);

        System.out.println("A subscrever na porta:\t" + args[0]);
        System.out.println("A receber queries na porta:\t" + args[1]);

        new Thread(cr).start();
        new Thread(cq).start();
    }
}