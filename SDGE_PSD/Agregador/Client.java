import Constants.QueryType;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

//Args: [SUB-PORT] [REQ-PORT]
public class Client {

    private static List<String> subscriptions;
    private static Scanner scin;
    private static ZMQ.Socket req;
    private static ZMQ.Socket sub;


    public Client(){
        scin = new Scanner(System.in);

    }


    public static void main(String[] args) throws Exception{
        subscriptions = new ArrayList<>();

        ZMQ.Context context = ZMQ.context(1);

        // ZeroMQ Socket para SUBSCRIBER
        sub = context.socket(SocketType.SUB);
        sub.connect("tcp://localhost:" + args[0]);

        // ZeroMQ Socket para REQUEST
        req = context.socket(SocketType.REQ);
        req.connect("tcp://localhost:" + args[1]);

        // Thread exclusiva para notificações
        ClientNotifier cr = new ClientNotifier(sub);
        new Thread(cr).start();

        menuPrincipal();


    }

    private static void menuPrincipal(){
        ClientView menu = new ClientView(new String[]{
                "Consultar estado global do sistema",
                "Subscrever Notificações",
                "Anular Subscrições"
        });

        menu.setHandler(1, Client::menuGlobalState);
        menu.setHandler(2, Client::menuSubs);
        menu.setHandler(3, Client::menuUnsubs);

        menu.executa(1);
    }

    private static void menuGlobalState() {
        ClientView menu = new ClientView(new String[]{
                "Número de dispositivos online (por Tipo).",
                "Verificar se dispositivo está online.",
                "Número de dispositivos ativos no sistema.",
                "Número de eventos ocorridos (por Tipo)."
        });

        menu.setHandler(1,Client::execOnlineDevicesType);
        menu.setHandler(2,Client::execCheckOnlineDevice);
        menu.setHandler(3,Client::execOnlineDevices);
        menu.setHandler(4,Client::execEventsNumberType);

        menu.executa(2);
    }

    /* - - - - - QUERIES - - - - - - */
    private static void execOnlineDevicesType() {

        System.out.print("Indique o tipo de dispositivo que pretende saber o número de dispositivos online:  ");
        String type = scin.nextLine();

        System.out.println("Número de dispositivos do tipo " + type + " online:" + doQuery(QueryType.QUERY_TOTAL_DEVICES_TYPE + " " +type));


    }

    private static void execCheckOnlineDevice() {
        System.out.print("Indique o identificador do dspositivo que pretende ver se está online: ");
        String id = scin.nextLine();
        System.out.println("Dispositivo está online no sistema:\t" + (doQuery(QueryType.QUERY_SPECIFIC_DEVICE + " " +id)));


    }

    private static void execOnlineDevices() {
        System.out.println("Número de dispositivos online no sistema:\t" + doQuery(QueryType.QUERY_TOTAL_DEVICES));
    }

    private static void execEventsNumberType() {
        // Fazer coisas
        System.out.print("Indique o tipo de evento que pretende saber o número:  ");
        String type = scin.nextLine();

        System.out.println("Número de eventos do tipo " + type + " ocorridos no sistema:" + doQuery(QueryType.QUERY_EVENT_NUMBER + " " + type));
    }

    /* - - - - - - - - - - - - - -- - - - */
    /* - - - - SUBSCRIÇÕES - - - - - */

    private static void menuSubs() {

        ClientView menu = new ClientView(new String[]{
                "Inexistência de dispositivos online na zona (Por Tipo)",
                "Recorde de dispositivos online",
                "Aumento da Percentagem de dispositivos online",
                "Diminuição da Percentagem de dispositivos online"
        });

        menu.setHandler(1, Client::subNoOnlineDevices);
        menu.setHandler(2, Client::subOnlineRecord);
        menu.setHandler(3, Client::subDevicesIncrease);
        menu.setHandler(4, Client::subDevicesDecrease);

        menu.executa(3);
    }

    private static void subNoOnlineDevices() { subscribe("TOPIC_TYPE_GONE"); }

    private static void subOnlineRecord() { subscribe("TOPIC_ONLINE_INCREASE");}

    private static void subDevicesIncrease() { subscribe("TOPIC_DEVICES_INCREASE"); }

    private static void subDevicesDecrease() { subscribe("TOPIC_DEVICES_DECREASE"); }



    /* -- - - - - - -- - - - */

    /* - - - ANULAR SUBSCRIÇÃO  - - -- -  */

    private static void menuUnsubs() {

        ClientView menu = new ClientView(new String[]{
                "Inexistência de dispositivos online na zona (Por Tipo)",
                "Recorde de dispositivos online",
                "Aumento da Percentagem de dispositivos online",
                "Diminuição da Percentagem de dispositivos online"
        });

        menu.setHandler(1, Client::unsubNoOnlineDevices);
        menu.setHandler(2, Client::unsubOnlineRecord);
        menu.setHandler(3, Client::unsubDevicesIncrease);
        menu.setHandler(4, Client::unsubDevicesDecrease);

        menu.executa(4);
    }



    private static void unsubNoOnlineDevices() { unsubscribe("TOPIC_TYPE_GONE"); }

    private static void unsubOnlineRecord() { unsubscribe("TOPIC_ONLINE_INCREASE"); }

    private static void unsubDevicesIncrease() { unsubscribe("TOPIC_DEVICES_INCREASE"); }

    private static void unsubDevicesDecrease() { unsubscribe("TOPIC_DEVICES_DECREASE"); }


    private static String doQuery(String queryType){
        req.send(queryType.getBytes(ZMQ.CHARSET));
        return new String(req.recv(),ZMQ.CHARSET); // TOPIC
    }

    private static void subscribe(String notif){
        if(!(subscriptions.contains(notif))){
            System.out.println("Subscrição efetuada com sucesso!");
            sub.subscribe(notif);
            subscriptions.add(notif);
        }
        else {
            System.out.println("Subscrição já efetuada!");
        }
    }


    private static void unsubscribe(String notif){
        if (subscriptions.isEmpty()) {
            System.out.println("Não tem nenhuma Notificação subscrita!");
        }
        else {
            if (subscriptions.contains(notif)) {
                System.out.println("Anulou a subscrição com sucesso!");
                sub.unsubscribe(notif);
                subscriptions.remove(notif);
            } else {
                System.out.println("ERRO: Não pode anular uma subscrição inexistente!");
            }
        }
    }

}