import org.zeromq.SocketType;
import org.zeromq.ZMQ;

public class Broker {
    public static void main(String[] args) {

        //Isto é tudo para o PUB/SUB -> Notificações
        ZMQ.Context context = ZMQ.context(1); //número de threads
        ZMQ.Socket front = context.socket(SocketType.SUB);
        ZMQ.Socket back = context.socket(SocketType.PUB);

        back.bind("tcp://*:"+args[0]);
        front.bind("tcp://*:"+args[1]);
        front.subscribe("TOPIC_1"); // IMPORTANT (TOPIC)

        System.out.println("Subscribers Port:\t" + args[0]);
        System.out.println("Publishers Port:\t" + args[1]);


        System.out.println("*** Broker Initialized ***");
        ZMQ.proxy(front, back, null);
    }
}
