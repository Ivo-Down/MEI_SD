package DataStructs;

import java.io.*;
import java.util.*;

public class Table implements Serializable {
    private final HashMap<Integer, Integer> neighbourNodes;  // id node -> porta pull node

    public Table(){
        this.neighbourNodes = new HashMap<>();
    }
    
    public void addNode( int port, int nodeId){
        neighbourNodes.put(nodeId, port);
    }

    public int getNodePort(int nodeId){
        return neighbourNodes.get(nodeId);
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