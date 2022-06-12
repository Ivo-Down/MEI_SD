import org.zeromq.ZMQ;

public class AggregatorQueries implements Runnable{
    private final ZMQ.Socket rep;
    private final Aggregator ag;

    public AggregatorQueries(ZMQ.Socket rep, Aggregator ag) {
        this.rep = rep;
        this.ag = ag;
    }

    public void run(){
        while(true){
            try {
                handleQuery();
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    private void handleQuery(){
        var query = new String(rep.recv(),ZMQ.CHARSET);
        var queryArgs = query.split(" ");
        switch (queryArgs[0]){
            case QueryType.QUERY_SPECIFIC_DEVICE:
                rep.send(HandleSpecificDeviceOnline(queryArgs).getBytes(ZMQ.CHARSET));
                break;
            case QueryType.QUERY_EVENT_NUMBER:
                rep.send(HandleEventTypeTotal(queryArgs).getBytes(ZMQ.CHARSET));
                break;
            case QueryType.QUERY_TOTAL_DEVICES:
                rep.send(HandleTotalDevicesOnline().getBytes(ZMQ.CHARSET));
                break;
            case QueryType.QUERY_TOTAL_DEVICES_TYPE:
                rep.send(HandleTotalDevicesOnlineOfType(queryArgs).getBytes(ZMQ.CHARSET));
                break;
            default:
                rep.send("ERROR: UNKNOWN QUERY!");
                break;
        }
    }
    private String HandleTotalDevicesOnline(){
        return String.valueOf(ag.getDevicesOnline());
    }
    private String HandleTotalDevicesOnlineOfType(String[] query){
        if(query.length<2) return "ERROR: INVALID ARGUMENTS!";
        return String.valueOf(ag.getDevicesOnlineOfType(query[1]));
    }
    private String HandleSpecificDeviceOnline(String[] query){
        if(query.length<2) return "ERROR: INVALID ARGUMENTS!";
        return String.valueOf(ag.getIsDeviceOnline(Integer.parseInt(query[1])));
    }
    private String HandleEventTypeTotal(String[] query){
        if(query.length<2) return "ERROR: INVALID ARGUMENTS!";
        else return String.valueOf(ag.getNumberEvents(query[1]));
    }
}
