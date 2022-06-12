import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class ClientView {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_BLUE = "\u001B[34m";

    public interface MenuHandler {
        void execute();
    }

    public interface MenuPreCondition {
        boolean validate();
    }

    // Varíavel de classe para suportar leitura
    private static final Scanner is = new Scanner(System.in);

    // variáveis de instância
    private final List<String> opcoes;
    private final List<MenuPreCondition> disponivel;  // Lista de pré-condições
    private final List<MenuHandler> handlers;         // Lista de handlers

    private int op;


    public ClientView(String[] opcoes) {
        this.opcoes = Arrays.asList(opcoes);
        this.disponivel = new ArrayList<>();
        this.handlers = new ArrayList<>();
        this.opcoes.forEach(s-> {
            this.disponivel.add(()->true);
            this.handlers.add(()->System.out.println("\nATENÇÃO: Opção não implementada!"));
        });

        this.op = 0;
    }

    public void executa(int k) {
        do {
            showMenu(k);
            this.op = lerOpcao();
            if (op>0 && !this.disponivel.get(op-1).validate()) {
                System.out.println(ANSI_RED + "Opção indisponível! Tente novamente." + ANSI_RESET);
            } else if (op>0) {
                this.handlers.get(op-1).execute();
            }
        } while (this.op != 0);
    }

    private void showTitle(int i){
        switch (i) {
            case 1:
                System.out.println(ANSI_BLUE + "------ Menu Inicial ------" + ANSI_RESET);
                break;
            case 2:
                System.out.println("------ Indique qual a query a executar! ------");
                break;
            case 3:
                System.out.println("------ Indique qual a notificação que pretende subscrever! ------");
                break;
            case 4:
                System.out.println("------ Indique qual a notificação que pretende CANCELAR a subscrição! ------");
            default:
                break;
        }
    }

    private void showMenu(int k) {
        showTitle(k);
        for (int i=0; i<this.opcoes.size(); i++) {
            System.out.print(i+1);
            System.out.print(" - ");
            System.out.println(this.opcoes.get(i));
        }
        System.out.println("0 - Sair.");
    }


    private int lerOpcao() {
        int op;

        System.out.print("Opção: ");
        op = is.nextInt();
        while ( op < 0 || op > this.opcoes.size()) {
            System.out.println(ANSI_RED + "Opção Inválida!" + ANSI_RESET);
            op = lerOpcao();
        }
        return op;
    }

    /**
     * Método para registar um handler numa opção do menu.
     *
     * @param i indice da opção  (começa em 1)
     * @param h handlers a registar
     */
    public void setHandler(int i, MenuHandler h) {
        this.handlers.set(i-1, h);
    }

}