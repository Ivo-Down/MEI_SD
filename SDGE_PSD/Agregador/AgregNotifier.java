import org.zeromq.ZMQ;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class AgregNotifier implements Runnable{
    private ZMQ.Socket pub;
    private Agregator district;

    public AgregNotifier(ZMQ.Socket pub, Agregator district) throws Exception {
        this.pub = pub;
        this.district = district;
    }

    public void run(){
        while(true){
           //Aqui uma função de receber e dar parse da mensagem
            try{
                pub.send("[Recebi esta mensagem da zona: " + district.getzoneName() + "]");
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }



}
