import org.zeromq.ZMQ;

// This module will send updates to other aggregators, connecting to its neighbours
public class AggregatorPush implements Runnable{
    private final ZMQ.Socket pub;
    private final Aggregator district;

    public AggregatorPush(ZMQ.Socket pub, Aggregator district) throws Exception {
        this.pub = pub;
        this.district = district;
    }

    public void run(){
        while(true){
           //Aqui uma função de receber e dar parse da mensagem
            try{
                String pushMessage = "[Update Message]";
                System.out.println("Sending an update: " + pushMessage);
                pub.send(pushMessage.getBytes(ZMQ.CHARSET));    // IMPORTANT (CONTENT)
                Thread.sleep(4000);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }



}
