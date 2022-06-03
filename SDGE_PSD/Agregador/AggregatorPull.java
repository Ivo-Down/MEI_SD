import org.zeromq.ZMQ;

// This module will receive updates from other aggregators, connecting to its neighbours, as well as collectors
public class AggregatorPull implements Runnable{
    private final ZMQ.Socket pub;
    private final Aggregator district;

    public AggregatorPull(ZMQ.Socket pub, Aggregator district) throws Exception {
        this.pub = pub;
        this.district = district;
    }

    public void run(){
        while(true){
           //Aqui uma função de receber e dar parse da mensagem
            try{
                String response = new String(pub.recv(),ZMQ.CHARSET);    // IMPORTANT (CONTENT)
                System.out.println("Received an update: " + response);
                Thread.sleep(2000);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }



}
