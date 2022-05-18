import org.zeromq.SocketType;
import org.zeromq.ZMQ;

public class Client {
    public static void main(String[] args) throws Exception{

        // ZeroMQ Socket para SUBSCRIBER
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket sub = context.socket(SocketType.SUB);
        sub.connect("tcp://localhost:" + args[0]);

        // ZeroMQ Socket para REQUEST


        ClientNotifier cr = new ClientNotifier(sub);
        System.out.println("A ouvir na porta " + args[0]);
        new Thread(cr).start();
    }
}