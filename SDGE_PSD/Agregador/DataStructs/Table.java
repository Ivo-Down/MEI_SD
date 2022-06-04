package DataStructs;

import java.io.*;
import java.util.*;

public class Table implements Serializable {
    //private static final long serialVersionUID = 1249369646723187393L;
    private final HashMap<Integer, Integer> neighborNodes;  // id node -> porta pull node

    public Table(){
        this.neighborNodes = new HashMap<>();
    }



    public void addNode( int port, int nodeId){
        neighborNodes.put(nodeId, port);
    }


    public int getNodeByPort(int port){
        for(Map.Entry<Integer, Integer> e: neighborNodes.entrySet()){
            if(e.getValue()==port)
                return e.getKey();
        }
        return -1;
    }


    public int getNodePort(int nodeId){
        return neighborNodes.get(nodeId);
    }


    public int getSize(){
        return this.neighborNodes.size();
    }


    public ArrayList<Integer> getNeighborNodes(){
        ArrayList<Integer> res = new ArrayList<>();
        for(Map.Entry<Integer, Integer> e: neighborNodes.entrySet()){
            res.add(e.getValue());
        }
        return res;
    }

    public HashMap<Integer, Integer> getMap(){
        return this.neighborNodes;
    }



    @Override
    public String toString() {
        return "Table{" +
                "neighborNodes=" + neighborNodes +
                '}';
    }

}