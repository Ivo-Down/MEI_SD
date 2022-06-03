import org.zeromq.ZMQ;

// This module will receive updates from other aggregators, connecting to its neighbours, as well as collectors
public class AggregatorPull implements Runnable{
    private final ZMQ.Socket pub;
    private final Aggregator district;

    //TODO: pensar em estruturas para guardar informações sobre notis. (Um id por tipo - 4 tipos diferentes (?))
    // Mudar os nomes destas cenas.

    public AggregatorPull(ZMQ.Socket pub, Aggregator district) throws Exception {
        this.pub = pub;
        this.district = district;
    }

    public void run(){
        while(true){
           //Aqui uma função de receber e dar parse da mensagem
            try{
                String response = new String(pub.recv(),ZMQ.CHARSET);    // IMPORTANT (CONTENT)


                /*
                    // Receber de um Agregador Ou Coletor.
                    // Agregador são estados;
                    // Coletor é tipo info.

                    - - - - - - UMA THREAD PARA ISTO:  - - - - - -

                    De agregadores só se recebe estados; Faz-se merge e SE ATUALIZAR, enviamos aos vizinhos (push para cada 1 dos sockets);

                    De coletores recebemos info sobre dispositivos ou eventos;
                            Atualizamos o nosso estado e enviamos a informação aos vizinhos (push para cada 1 dos sockets);
                                    - Se for eventos, não se dá logo broadcast;
                                    - Se for sobre dispositivos, dá-se broadcast.

                    - - - - - - - - -

                        A SEGUNDA  Thread, faz SÓ a disseminação "epidémica" do estado para os vizinhos (caso estes se tenham desligado).

                    - - - - - - - - -

                 */

                System.out.println("Received an update: " + response);
                Thread.sleep(2000);
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }



}