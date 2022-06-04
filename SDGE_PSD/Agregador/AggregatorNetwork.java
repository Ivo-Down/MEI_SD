import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

// This module will receive updates from other aggregators, connecting to its neighbours, as well as collectors
public class AggregatorNetwork implements Runnable{
    private final ZMQ.Socket pub;
    private final Aggregator aggregator;

    //TODO: pensar em estruturas para guardar informações sobre notis. (Um id por tipo - 4 tipos diferentes (?))
    // Mudar os nomes destas cenas.

    public AggregatorNetwork(ZMQ.Socket pub, Aggregator aggregator) throws Exception {
        this.pub = pub;
        this.aggregator = aggregator;
    }

    public void run(){

        while(true){
           //Aqui uma função de receber e dar parse da mensagem
            try{
                //String response = new String(pub.recv(),ZMQ.CHARSET); // IMPORTANT (CONTENT)
                //byte[] data  = pub.recv();

                ZMsg msg = ZMsg.recvMsg(this.pub);
                System.out.println("received msg");

                String aux = new String(msg.pop().getData(),ZMQ.CHARSET);

                System.out.println(aux);

                if(aux.equals("A")){
                    System.out.println("Estado de agregador recebida.");
                    StateCRDT state = (StateCRDT) StateCRDT.deserialize(msg.pop().getData());

                    if (this.aggregator.merge(state)){
                        this.aggregator.propagateState();
                    }

                    //System.out.println(state.toString());


                } else if (aux.equals("C")) {
                    // Receber +1 frame que identifica o tipo de not.

                    String notificationType = new String(msg.pop().getData(),ZMQ.CHARSET);

                    if (notificationType.equals("D")){
                        // TODO: Adicionar informação do dispositivo (Fazer métodos);
                        this.aggregator.propagateState();

                    } else if (notificationType.equals("E")) {
                        // TODO: Adicionar informação de eventos.
                    }
                }

                /*
                    // Receber de um Agregador Ou Coletor.
                    // Agregador são estados;
                    // Coletor é tipo info.

                    - - - - - - UMA THREAD PARA ISTO:  - - - - - -

                     - - De agregadores só se recebe estados; Faz-se merge e SE ATUALIZAR, enviamos aos vizinhos (push para cada 1 dos sockets);

                    De coletores recebemos info sobre dispositivos ou eventos;
                            Atualizamos o nosso estado e enviamos a informação aos vizinhos (push para cada 1 dos sockets);
                                    - Se for eventos, não se dá logo broadcast;
                                    - Se for sobre dispositivos, dá-se broadcast.
                 */




                //System.out.println("Received an update: " + response);
                Thread.sleep(2000);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }



}