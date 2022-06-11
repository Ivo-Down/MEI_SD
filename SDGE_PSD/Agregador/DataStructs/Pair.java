package DataStructs;

import java.io.Serializable;

public class Pair implements Serializable {
    private Integer p1;
    private Integer p2;

    public Pair(){
        this.p1 = 0;
        this.p2 = 0;
    }

    public Pair(Integer p1, Integer p2){
        this.p1 = p1;
        this.p2 = p2;
    }

    public Pair(Pair old){
        this.p1 = old.p1;
        this.p2 = old.p2;
    }

    public Integer getFst(){
        return this.p1;
    }

    public Integer getSnd(){
        return this.p2;
    }

    public Integer getPairValue() {return this.p1 - this.p2;}

    public void addToFst(Integer v){ this.p1 += v; }

    public void addToSnd(Integer v){ this.p2 += v; }

}
