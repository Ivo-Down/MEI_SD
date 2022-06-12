import DataStructs.Table;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

// Args [BOOT_PORT]
public class BootStrapper {
    private static ZMQ.Socket rep;
    private static Table overlayNodes;

    public static final int bootstrapper_port = 8888;

    public static void main(String[] args) {
        overlayNodes = processJsonData();

        ZMQ.Context context = ZMQ.context(1);

        // ZeroMQ para REPLY
        rep = context.socket(SocketType.REP);
        rep.bind("tcp://localhost:" + bootstrapper_port);
        while(true){
            handleRequest();
        }
    }

    private static void handleRequest() {
        int nodeId = -1;
        //Partir a request para buscar id
        System.out.println("BOOTSTRAPPER - Handling neighbour request");
        String id = new String(rep.recv(),ZMQ.CHARSET); //"Id"
        System.out.println("ID: \t" + id);


        //Calcular vizinhos
        Table requestedNeighbors = getNeighbors(Integer.parseInt(id));
        byte[] data = StaticMethods.serialize(requestedNeighbors);

        //serializar info
        rep.send(data);
    }

    private static Table processJsonData(){
        JSONParser parser = new JSONParser();
        Table res = new Table();
        try {
            JSONArray jsonArray =  (JSONArray) parser.parse(new FileReader("overlay.json"));

            for (Object o: jsonArray){
                JSONObject node = (JSONObject) o;
                int id = Integer.parseInt((String) node.get("node"));
                int node_port = Integer.parseInt((String) node.get("port"));;

                res.addNode(node_port, id);

            }
        } catch (IOException | ParseException e){
            e.printStackTrace();
        }

        return res;
    }
    private static Table getNeighbors(int nodeId) {
        JSONParser parser = new JSONParser();
        ArrayList<Integer> neighbours = new ArrayList<>();
        Table res = new Table();
        try {
            JSONArray jsonArray = (JSONArray) parser.parse(new FileReader("overlay.json"));

            for (Object o : jsonArray) {
                JSONObject node = (JSONObject) o;
                int id = Integer.parseInt((String) node.get("node"));

                if (nodeId == id) {
                    JSONArray neighboursJson = (JSONArray) node.get("neighbors");

                    for (Object obj : neighboursJson) {
                        Integer i = ((Long) obj).intValue();
                        neighbours.add(i);
                    }
                    break;
                }
            }
            for (Integer i : neighbours) {
                int pullport = overlayNodes.getNodePort(i);
                res.addNode(i, pullport);
            }

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

        return res;
    }
}
