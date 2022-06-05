import org.zeromq.ZMQ;

// This module will send updates to other aggregators, connecting to its neighbours
public class AggregatorReceiver implements Runnable{
    private final ZMQ.Socket push;
    private final ZMQ.Socket pull;
    private final Aggregator ag;

    public AggregatorReceiver(ZMQ.Socket push, ZMQ.Socket pull, Aggregator ag) throws Exception {
        this.push = push;
        this.pull = pull;
        this.ag = ag;
    }

    public void run(){
        while(true){
           //Aqui uma função de receber e dar parse da mensagem
            try{
                String request = new String(pull.recv(),ZMQ.CHARSET);
                System.out.println("Pulled request:\t" + request);
                String pushMessage = "[Update Message]";
                push.send(pushMessage.getBytes(ZMQ.CHARSET));    // IMPORTANT (CONTENT)
                System.out.println("Pushed response: " + pushMessage);
                Thread.sleep(4000);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }



}
