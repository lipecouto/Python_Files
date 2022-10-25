# Importador automático de dados do IGPM para banco de dados SQL SERVER

### 👋 Olá sou Philipe, e esse é um projeto novo que estou implementando, atuei como profissional especialista no ERP Sankhya por 12 anos e agora decidi mudar para desenvolvedor soluções diversas para problemas diversos.

---

#### Esse repositório tem como objetivo ajudar as pessoas que eventualmente precisam de uma solução para atualizar os dados de IGPM no sistema Sankhya.


### Informações importantes sobre esse projeto
- Para funcionar, é necessário baixar todos os arquivos do repositório.
- É necessário ter Python 3.8 ou superior.
- É necessário editar o arquivo config.ini e preencher os dados solicitados.
- O arquivo .so deverá ficar em um diretório onde poderá ser acessado pela aplicação.
- A aplicação busca os dados de IGPM de um host externo gratuito, não há garantias de que esse host sempre irá funcionar, fique atento a isso.
- É necessário baixar as librarys indicadas no arquivo .py, sem elas a aplicação não funcionará.
- Uma tabela adicional será criada no seu banco de dados caso ela não exista, logo, os dados devem ser extraidos desta tabela e distribuidos nas tabelas do sistema. (podemos montar uma procedure para isso posteriormente, sempre verifique esse repositório)
- Qualque dúvida ou sugestão, sinta-se a vontade para entrar em contato.
- Esse código é livre então fique a vontade para baixar o repositório e alterar o que lhe for conveniênte.