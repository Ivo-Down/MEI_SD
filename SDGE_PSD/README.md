Para correr o coletor com dispositivos a ligarem-se e enviarem eventos:

  - 2 consolas com erl abertas
  - numa delas, c(coletor_ivo).  e depois  coletor_ivo:start(12345).      (12345 é uma porta à sorte)
  - na outra,   c(super_client). e depois  super_client:start(12345,N).   (N é o numero de devices a gerar)