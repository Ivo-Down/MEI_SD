import org.zeromq.ZMQ;

public class AggregatorQueries implements Runnable{
    private final ZMQ.Socket req;
    private final Aggregator ag;

    public AggregatorQueries(ZMQ.Socket req, Aggregator ag) throws Exception {
        this.req = req;
        this.ag = ag;
    }

    public void run(){
        String res;
        while(true){
           //Aqui uma função de receber e dar parse da mensagem
            try{
                System.out.println("Recebi o pedido:\t" + new String(req.recv(),ZMQ.CHARSET)); // TOPIC
                res = "1 + 1 = 2!";
                req.send(res.getBytes(ZMQ.CHARSET));    // IMPORTANT (CONTENT)
                Thread.sleep(2000);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }



}
