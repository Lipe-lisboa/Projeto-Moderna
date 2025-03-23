# Parquet 

Apache Parquet é um formato de arquivo de código aberto e orientado por colunas.

O formato de armazenamento colunar torna o Parquet bastante eficiente quando se trata de armazenar e analisar grandes volumes de dados. Isso porque, ao executar uma consulta, podemos nos concentrar apenas nas informações relevantes minimizando bastante a quantidade de dados que precisam ser processados.

Outra característica interessante deste formato é a possibilidade de utilizarmos esquemas de compressão específicos para cada coluna. Este recurso aumenta de forma significativa a eficiência e a economia com recursos de armazenamento.


# ZIP

Um arquivo ZIP é um formato de compactação usado para armazenar um ou mais arquivos dentro de um único arquivo compactado, reduzindo seu tamanho e facilitando o armazenamento e a transferência. Ele usa compressão sem perda de qualidade, ou seja, os arquivos podem ser restaurados exatamente como eram antes da compactação.

Para que serve um arquivo ZIP?
Economizar espaço – Reduz o tamanho de arquivos grandes, economizando armazenamento.
Facilitar o envio e o download – Agrupa vários arquivos em um único pacote, tornando mais prático o compartilhamento por e-mail, sites ou nuvem.
Acelerar a transferência – Arquivos menores são enviados e baixados mais rapidamente.
Organização – Permite agrupar vários arquivos em um único lugar sem precisar de várias pastas.
Segurança – Pode ser protegido com senha para restringir o acesso.


# CSV
Um arquivo CSV (Comma-Separated Values, ou Valores Separados por Vírgula) é um formato simples de arquivo usado para armazenar dados tabulares, como planilhas ou bancos de dados, em texto puro.

Características de um CSV:
✅ Formato Simples – Cada linha representa um registro, e os valores são separados por vírgulas (,), ponto e vírgula (;) ou tabulação (\t).
✅ Fácil de Ler e Escrever – Pode ser aberto em Excel, Google Sheets, Notepad, e lido por várias linguagens de programação.
✅ Portável – Compatível com a maioria dos softwares de análise de dados e bancos de dados.

# Pandas
A biblioteca C é uma biblioteca do Python usada para manipulação, análise e visualização de dados. Ela permite trabalhar com dados estruturados, como tabelas (semelhantes às do Excel ou SQL), de forma eficiente e fácil.

Principais Funcionalidades do Pandas
✅ Leitura e Escrita de Arquivos – CSV, Excel, JSON, SQL, entre outros.
✅ Manipulação de Dados – Filtragem, agrupamento, ordenação e transformação de tabelas.
✅ Análise Estatística – Médias, somas, contagens e outras operações numéricas.
✅ Visualização de Dados – Integração com bibliotecas como Matplotlib e Seaborn.


# FastAPI
GET:

Utilizado para solicitar dados de um servidor.
É uma operação de leitura, ou seja, não deve modificar os dados no servidor.
Exemplo: obter informações de um produto, listar todos os usuários.

PUT:

Utilizado para atualizar um recurso existente no servidor.
Envia os dados completos do recurso atualizado.
Exemplo: atualizar os dados de um usuário específico.

POST:

Utilizado para enviar dados para o servidor, geralmente para criar um novo recurso.
Exemplo: criar um novo usuário, adicionar um novo produto.

