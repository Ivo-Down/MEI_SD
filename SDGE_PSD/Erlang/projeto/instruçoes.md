projeto
=====

- Adicionar as dependências no rebar.config, neste momento so temos a de jsx
- Os ficheiros externos ao projeto têm que estar fora da pasta src

Build
-----
    Para compilar:

    $ rebar3 compile

    Para abrir a shell:

    $ rebar3 shell

    Para sair: ctrl + G e depois $ q
    

Para correr abrir um terminal para devices e um terminal por cada coletor:
-----
    $ rebar3 compile
    
    $ rebar3 shell


Para coletor:
-----
    $ c(coletor).
    $ coletor:start(PortNumber).   -> no futuro vai levar tb porta do agregador
    
Para devices:
-----
    $ c(devices).
    
    $ devices:start([ColPortNumber1, ColPortNumber2, ...]).
