package DataStructs;

import java.io.*;
import java.util.*;

public class Table implements Serializable {
    //private static final long serialVersionUID = 1249369646723187393L;
    private final HashMap<Integer, Integer> neighbourNodes;  // id node -> porta pull node

    public Table(){
        this.neighbourNodes = new HashMap<>();
    }



    public void addNode( int port, int nodeId){
        neighbourNodes.put(nodeId, port);
    }


    public int getNodeByPort(int port){
        for(Map.Entry<Integer, Integer> e: neighbourNodes.entrySet()){
            if(e.getValue()==port)
                return e.getKey();
        }
        return -1;
    }


    public int getNodePort(int nodeId){
        return neighbourNodes.get(nodeId);
    }


    public int getSize(){
        return this.neighbourNodes.size();
    }


    public ArrayList<Integer> getNeighbourNodes(){
        ArrayList<Integer> res = new ArrayList<>();
        for(Map.Entry<Integer, Integer> e: neighbourNodes.entrySet()){
            res.add(e.getValue());
        }
        return res;
    }

    public HashMap<Integer, Integer> getMap(){
        return this.neighbourNodes;
    }



    @Override
    public String toString() {
        return "Table{" +
                "neighborNodes=" + neighbourNodes +
                '}';
    }

}