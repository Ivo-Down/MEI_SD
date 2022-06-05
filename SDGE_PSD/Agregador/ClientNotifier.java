import org.zeromq.ZMQ;

public class ClientNotifier implements Runnable{
    private ZMQ.Socket sub;

    // ESTA CLASSE VAI SER VITIMA DE UM POLIMORFISMO QUE ATÉ VAI DOER
    // VAI SER ADICIONADO AQUI TAMBÉM O REQUEST
    public ClientNotifier(ZMQ.Socket sub) {
        this.sub = sub;
    }

    public void run() {
        System.out.println("Client Notification Handler Initialized!");
        System.out.println("Waiting for notifications...");
        while(!Thread.currentThread().isInterrupted()) {
            System.out.println("Client received notification with topic:\t" + new String(sub.recv(),ZMQ.CHARSET)); // TOPIC
            System.out.println("Client received notification with content:\t" + new String(sub.recv(),ZMQ.CHARSET)); // DATA

        }
    }
}