leitor
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
    

Interpretador de JSON
-----
    $ rebar3 compile
    
    $ rebar3 shell
    
    $ c(jsoninterpreter).
    
    $ jsoninterpreter:parse_file("nomeficheiro,json").
