import org.zeromq.ZMQ;

public class ClientQueries implements Runnable{
    private ZMQ.Socket req;

    // ESTA CLASSE VAI SER VITIMA DE UM POLIMORFISMO QUE ATÉ VAI DOER
    // VAI SER ADICIONADO AQUI TAMBÉM O REQUEST
    public ClientQueries(ZMQ.Socket req) {
        this.req = req;
    }

    public void run() {
        String res;
        while(!Thread.currentThread().isInterrupted()) {
            res = "Vou fazer um Request";
            req.send(res.getBytes(ZMQ.CHARSET));

            System.out.println("Recebi uma resposta da querie :\t" + new String(req.recv(),ZMQ.CHARSET)); // TOPIC
        }
    }
}