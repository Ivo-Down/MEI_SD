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
public class BootSrapper {
    private static Table table;
    private static ZMQ.Socket rep;

    // Ler os dados do json
    // Fazer pre processamento
    // Criar socket tipo rep
    // Receber mensagem tipo req
    // Retirar da mensagem id do agregador que enviou
    // Ir buscar os vizinhos desse id
    // Processo de serializar a table
    // Enviar pelo socket rep
    public static void main(String[] args) {
        //table = processJsonData();

        ZMQ.Context context = ZMQ.context(1);

        // ZeroMQ para REPLY
        rep = context.socket(SocketType.REP);
        rep.bind("tcp://localhost:" + args[0]);
        while(true){
            handleRequest();
        }
    }

    private static void handleRequest() {
        int nodeId = -1;
        //Table nInfo = getNeighbors(nodeId);
        String id = new String(rep.recv(),ZMQ.CHARSET);
        String content = new String(rep.recv(),ZMQ.CHARSET);
        System.out.println("Id: \t" + id);
        System.out.println("Content: \t" + content);

        //Partir a request para buscar id

        //Calcular vizinhos


        //serializar info
        rep.sendMore("Neighbour".getBytes(ZMQ.CHARSET));
        rep.send("Port".getBytes(ZMQ.CHARSET));
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
                int pullport = table.getNodePort(i);
                res.addNode(i, pullport);
            }

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

        return res;
    }
}
