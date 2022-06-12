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
    private final Aggregator aggregator;
    private final AggregatorNotifier aggregatorNotifier;


    public AggregatorNetwork(ZMQ.Socket pull, ZMQ.Socket pub, Aggregator aggregator){
        this.pull = pull;
        this.aggregator = aggregator;
        this.aggregatorNotifier = new AggregatorNotifier(pub, aggregator.getId());
    }

    public void run(){
        BlockingQueue<Runnable> taskQueue = new ArrayBlockingQueue(512,true);
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(2, 4, 20, TimeUnit.SECONDS,taskQueue);

        threadPool.prestartAllCoreThreads();
        while(true){
            try{
                ZMsg msg = ZMsg.recvMsg(this.pull);

                String aux = new String(msg.pop().getData(),ZMQ.CHARSET);

                taskQueue.offer(new Task(aux, this.aggregator, this.aggregatorNotifier, msg));
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }



}