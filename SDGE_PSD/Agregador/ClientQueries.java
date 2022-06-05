import Constants.QueryType;
import org.zeromq.ZMQ;

public class ClientQueries implements Runnable{
    private final ZMQ.Socket req;

    // ESTA CLASSE VAI SER VITIMA DE UM POLIMORFISMO QUE ATÉ VAI DOER
    // VAI SER ADICIONADO AQUI TAMBÉM O REQUEST
    public ClientQueries(ZMQ.Socket req) {
        this.req = req;
    }

    public void run() {
        while(!Thread.currentThread().isInterrupted()) {
            System.out.println("Starting Queries...");

            System.out.println("Number of devices of a type online:\t" + doQuery(QueryType.QUERY_TOTAL_DEVICES_TYPE + " deviceType"));
            System.out.println("Number of devices online:\t" + doQuery(QueryType.QUERY_TOTAL_DEVICES));
            System.out.println("Device is active:\t" + doQuery(QueryType.QUERY_SPECIFIC_DEVICE + " 123"));
            System.out.println("Number of events of type:\t" + doQuery(QueryType.QUERY_EVENT_NUMBER + " eventType"));

            System.out.println("Queries complete...");
        }
    }

    private String doQuery(String queryType){
        req.send(queryType.getBytes(ZMQ.CHARSET));
        return new String(req.recv(),ZMQ.CHARSET); // TOPIC
    }
}