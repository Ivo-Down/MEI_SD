import org.zeromq.ZMQ;

public class ClientNotifier implements Runnable{
    private ZMQ.Socket sub;

    // ESTA CLASSE VAI SER VITIMA DE UM POLIMORFISMO QUE ATÉ VAI DOER
    // VAI SER ADICIONADO AQUI TAMBÉM O REQUEST
    public ClientNotifier(ZMQ.Socket sub) {
        this.sub = sub;
    }

    public void run() {
        String res;
        while(!Thread.currentThread().isInterrupted()) {
            //System.out.println("arroz");
            System.out.println("Topic:\t" + new String(sub.recv(),ZMQ.CHARSET)); // TOPIC
            System.out.println("Data:\t" + new String(sub.recv(),ZMQ.CHARSET)); // DATA
        }
    }
}