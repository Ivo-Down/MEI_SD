import org.zeromq.ZMQ;

public class AggregatorNotifier implements Runnable{
    private final ZMQ.Socket pub;
    private final Aggregator district;

    public AggregatorNotifier(ZMQ.Socket pub, Aggregator district) throws Exception {
        this.pub = pub;
        this.district = district;
    }

    public void run(){
        while(true){
           //Aqui uma função de receber e dar parse da mensagem
            try{
                System.out.println("Notifying connected clients...");
                String response = "[Recebi esta mensagem da zona: " + district.getzoneName() + "]";
                pub.sendMore("TOPIC_1");                // IMPORTANT (TOPIC)
                pub.send(response.getBytes(ZMQ.CHARSET));    // IMPORTANT (CONTENT)
                Thread.sleep(2000);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }



}
