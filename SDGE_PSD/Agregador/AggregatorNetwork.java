import com.ericsson.otp.erlang.*;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

// This module will receive updates from other aggregators, connecting to its neighbours, as well as collectors
public class AggregatorNetwork implements Runnable{
    private final ZMQ.Socket pull;
    private final ZMQ.Socket push;
    private final Aggregator aggregator;
    private final AggregatorNotifier aggregatorNotifier;

    //TODO: pensar em estruturas para guardar informações sobre notis. (Um id por tipo - 4 tipos diferentes (?))
    // Mudar os nomes destas cenas.

    public AggregatorNetwork(ZMQ.Socket pull, ZMQ.Socket push, ZMQ.Socket pub, Aggregator aggregator) throws Exception {
        this.pull = pull;
        this.push = push;
        this.aggregator = aggregator;
        this.aggregatorNotifier = new AggregatorNotifier(pub, aggregator.getId());
    }

    public void run(){
        BlockingQueue<Runnable> taskQueue = new ArrayBlockingQueue(512,true);
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(2, 4, 20, TimeUnit.SECONDS,taskQueue);

        threadPool.prestartAllCoreThreads();
        Integer count = 0;
        while(true){
           //Aqui uma função de receber e dar parse da mensagem
            try{

                ZMsg msg = ZMsg.recvMsg(this.pull);
                //System.out.println("Pulled request:\t" + msg.toString());

                String aux = new String(msg.pop().getData(),ZMQ.CHARSET);

                //System.out.println(aux);

                taskQueue.offer(new Task(aux, this.aggregator, this.aggregatorNotifier, msg, count));

                //Thread.sleep(1000);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }



}