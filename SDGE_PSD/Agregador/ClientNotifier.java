import org.zeromq.ZMQ;

public class ClientNotifier implements Runnable{
    private ZMQ.Socket sub;

    // ESTA CLASSE VAI SER VITIMA DE UM POLIMORFISMO QUE ATÉ VAI DOER
    // VAI SER ADICIONADO AQUI TAMBÉM O REQUEST
    public ClientNotifier(ZMQ.Socket sub) {
        this.sub = sub;
    }

    public void run() {
        while(true) {
            //System.out.println("arroz");
            System.out.println(new String(sub.recv()));
        }
    }
}