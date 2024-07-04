A ideia aqui é criarmos um ambiente de desenvolvimento local do AWS Glue partindo da imagem Docker amazon/aws-glue-libsaws-glue-libs. Essa imagem suporta o Glue 4.0 e Pyspark 3.*.*.

Depois vamos configurar duas opções para você desenvolver e testar seus programas, uma pelo VSCode e outra pelo Jupyter Notebook.

O melhor de tudo!? Execute quantas vezes quiser sem se preocupar com o custo. Você nem precisa de uma conta AWS se não for consumir nenhum recurso lá.

Ideal para inicar seu aprendizado!

Pré-requisitos

Só para constar, todos os passos descritos nesse tutorial foram executados no Ubuntu 20.04, porém isso não é um pré-requisito. Você pode utilizar outro sistema operacional de sua preferência, adaptando alguns passos e instalando os pacotes de software na versão necessária.

Antes do primeiro passo, você vai precisar do Docker e VSCode instalados. Se ainda não tem esses caras, sugiro seguir os links oficiais abaixo. A ideia é muito simples.

Com o Docker vamos baixar a imagem e “levantar” o container. Nosso ambiente para execução dos jobs Glue.

Docker
Com o VSCode vamos nos conectar ao container para desenvolver e executar nossos jobs.

## No VSCode

Passo opcional

Com o AWS-CLI você poderá configurar uma conta AWS e prepar seu ambiente local para interagir com os recursos na cloud. Assim é possível por exemplo executar no seu desktop um job Glue que lê ou grava dados em um bucket S3 da sua conta AWS. Apesar de não ser essencial para esse tutorial, recomendo seguir esse passo para deixar o seu ambiente em ponto de bala.

AWS-CLI

AWS Glue e Pyspark no VSCode
Com o Docker instalado, abra o terminal e execute o comando abaixo para download da imagem:

```sudo docker pull amazon/aws-glue-libs:glue_libs_1.0.0_image_01```

Agora inicie o container com os serviços do Glue e PySpark:

```sudo docker run -itd -p 8888:8888 -p 4040:4040 -v ~/.aws:/root/.aws:ro -v ~/projetos:/home/projetos --name glue amazon/aws-glue-libs:glue_libs_1.0.0_image_01```

onde:

- *-v ~/.aws:/root/.aws:ro*: mapeia as credenciais AWS do host local para o container.

- *-v ~/projetos:/home/projetos*: mapeia o diretório no host local para pasta projetos no container.

Conectando VSCode ao Container
Abra o VSCode e instale as extensões python e remote-containers:

```
    ms-vscode-remote.remote-containers
    ms-python.python
```

Com as extensões instaladas, vamos conectar o VSCode ao nosso container.

- Clique no ícone verde do canto inferior esquerdo da tela.

- Será exibido um menu no centro da tela, próximo à barra de título.

- Selecione a opção Attach to Running Container e clique no nome do container.


Para testar nosso ambiente, crie um arquivo na pasta projetos com o nome glue_example.py e o seguinte código:


Agora vamos executar esse programa.

Abra o terminal no VSCode clicando em Terminal e New Terminal.


- Execute o comando:

```spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8/bin/spark-submit projetos/glue_example.py```


Após execução, você poderá conferir na saída do programa o print do schema e dos dados.

Você ainda pode usar como opção o terminal iterativo do Pyspark executando o comando a seguir:

spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8/bin/pyspark


### Encerrando os trabalhos

~ Desconecte o VSCode do container clicando no ícone verde do canto inferior esquerdo e na opção Close Remote Connection. ~

No terminal do sistema operacional, finalize a execução do container com o comando:

```sudo docker stop glue```

Você pode conferir o arquivo criado no seu diretório local. Para isso execute o seguinte comando no terminal do sistema operacional:

```ls -la ~/projetos```

Para usar novamente o ambiente é só reiniciar o container.

```sudo docker start glue```

Depois reconectá-lo ao VSCode.

## AWS Glue e Pyspark no Jupyter Notebook

Um super benefício dessa imagem Docker *amazon/aws-glue-libsaws-glue-libs* é a opção de utilizar o Jupyter.

Para isso vamos “subir” um segundo container com os parâmetros necessários para iniciar esse serviço.

No terminal do sistema operacional, execute o seguinte comando:

***
```sudo docker run -itd -p 8888:8888 -p 4040:4040 -v ~/.aws:/root/.aws:ro -v ~/projetos/jupyter:/home/jupyter/jupyter_default_dir --name glue_jupyter amazon/aws-glue-libs:glue_libs_1.0.0_image_01 /home/jupyter/jupyter_start.sh```
***

onde:

- *-v ~/.aws:/root/.aws:ro*: mapeia as credenciais AWS do host local para o container.

- *-v ~/projetos/jupyter:/home/jupyter/jupyter_default_dir*: mapeia diretório no host local para armazenar os notebooks criados.

- */home/jupyter/jupyter_start.sh*: inicia o serviço do Jupyter.

Agora é só abrir o browser na estação local e acessar o seguinte endereço:

- *http://127.0.0.1:8888*

Para testar, você pode utilizar o mesmo código sugerido para o VSCode.


### Encerrando os trabalhos

No terminal do sistema operacional, finalize a execução do container com o comando:

```sudo docker stop glue_jupyter```

Você pode conferir os arquivos criados no seu diretório local:

```ls -la ~/projetos/jupyter/```

- Para subir novamente o serviço do Jupyter:

```sudo docker start glue_jupyter```

### Conclusão

E é isso. Espero que esse tutorial te ajude tanto nos primeiros passos com o Glue quanto em economizar boas “doletas” no seu processo de desenvolvimento.

Até mais!