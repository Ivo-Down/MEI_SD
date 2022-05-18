import org.zeromq.SocketType;
import org.zeromq.ZMQ;

public class Broker {
    public static void main(String[] args) {

        //Isto é tudo para o PUB/SUB -> Notificações
        ZMQ.Context context = ZMQ.context(1); //número de threads
        ZMQ.Socket pubs = context.socket(SocketType.XSUB);
        ZMQ.Socket subs = context.socket(SocketType.XPUB);
        subs.bind("tcp://*:"+args[0]);
        pubs.bind("tcp://*:"+args[1]);

        System.out.println("Porta para os Publishers: " + args[1]);
        System.out.println("Porta para os Subscribers em: " + args[0]);


        System.out.println("*** O broker está ativo ***");
        ZMQ.proxy(pubs, subs, null);
    }
}
