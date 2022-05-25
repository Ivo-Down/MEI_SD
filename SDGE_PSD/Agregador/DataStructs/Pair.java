package DataStructs;

public class Pair {
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

    public Integer getFst(){
        return this.p1;
    }

    public Integer getSnd(){
        return this.p2;
    }
}
