import org.zeromq.ZMQ;

public class ClientNotifier implements Runnable{
    private ZMQ.Socket sub;
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public ClientNotifier(ZMQ.Socket sub) {
        this.sub = sub;
    }

    public void run() {
        System.out.println("Waiting for notifications...");
        while(!Thread.currentThread().isInterrupted() ) {
            sub.recv(); //debug receive
            while (sub.hasReceiveMore()) {
                System.out.println(ANSI_YELLOW + "*** NOTIFICAÇÃO RECEBIDA:\t" + new String(sub.recv(), ZMQ.CHARSET) + "***" + ANSI_RESET); // DATA
            }
        }
    }
}