
public class Aggregator {
    private final String zoneName;
    private final int id;

    public Aggregator(String zoneName, int id){
        this.zoneName = zoneName;
        this.id = id;
    }
    public String getzoneName(){
        return zoneName;
    }

    public int getId(){
        return id;
    }
}
