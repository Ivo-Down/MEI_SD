
public class Agregator {
    private final String zoneName;
    private int id;

    public Agregator(String zoneName, int id){
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
