import org.zeromq.SocketType;
import org.zeromq.ZMQ;

public class Client {
    public static void main(String[] args) throws Exception{

        // ZeroMQ Socket para SUBSCRIBER
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket sub = context.socket(SocketType.SUB);
        sub.connect("tcp://localhost:" + args[0]);
        sub.subscribe("TOPIC_1");   // IMPORTANT (TOPIC)

        // ZeroMQ Socket para REQUEST


        ClientNotifier cr = new ClientNotifier(sub);
        System.out.println("Listening on port:\t" + args[0]);
        new Thread(cr).start();
    }
}