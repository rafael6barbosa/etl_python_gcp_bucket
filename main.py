from google.cloud import storage, bigquery
from io import BytesIO
import pandas as pd
from datetime import datetime
from pytz import timezone

def carga_xlsx(self):
    data_carga = datetime.now(timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S")

    # Cria o client do serviço GCP:
    bigquery_client = bigquery.Client.from_service_account_json('C:/Users/supor_b755x4w/Downloads/pentahoBeams/pentahoBeams/service-accounts/clk-ope-pagseguro.json')
    storage_client = storage.Client.from_service_account_json('C:/Users/supor_b755x4w/Downloads/pentahoBeams/pentahoBeams/service-accounts/clk-ope-pagseguro.json')

    # Prepara as referências do dataset:
    table_id = "clk-ope-pagseguro.DEV.CARGA_TRACKSALES_COMMENTS_REVIEW"

    # Trunca a tabela destino:
    #bigquery_client.query("TRUNCATE TABLE clk-ope-pagseguro.DEV.CARGA_TRACKSALES_COMMENTS_REVIEW")
    arquivos_base = (bigquery_client.query("SELECT DISTINCT ARQUIVO FROM clk-ope-pagseguro.DEV.CARGA_TRACKSALES_COMMENTS_REVIEW")
    .result()
    .to_dataframe(
        create_bqstorage_client=True,
    )
    )

    # Prepara as referências do repositório:
    bucket = storage_client.get_bucket('pagseguro-atendimento')
    blobs = storage_client.list_blobs(bucket)
    filtered = filter(lambda blob: blob.name.startswith('tracksales/commentsReview/') and blob.name not in arquivos_base.values, blobs)

    job_config = bigquery.LoadJobConfig(
    schema=[],
    write_disposition="WRITE_APPEND",
    )

    for blob in filtered:  
        # Pega o arquivo em blob:
        blob = bucket.get_blob(blob.name)

        #Faz o dowload em string e insere em um data frame:
        content = blob.download_as_string()
        df = pd.read_excel(BytesIO(content))
        df.rename(columns={'#':'id', 'Lote do Disparo':'lote_disparo', 'Data/Hora do Disparo':'data_hora__disparo', 'Data/Hora da Opinião':'dta_hora_opiniao', 'Campanha':'campanha', 'Pergunta':'pergunta', 'Coleta':'coleta', 'Nome':'nome', 'Identificador':'identificador', 'E-mail':'email', 'E-mail Alternativo':'email_alternativo', 'Telefone':'telefone', 'Telefone Alternativo':'telefone_alternativo', 'Tempo de Resposta':'tempo_de_resposta', 'Identificação do Chamado':'identificacao_do_chamado', 'Identificação do Workflow':'identificacao_do_workflow', 'Data e Hora de Abertura':'data_hora_abertura', 'Assunto':'assunto', 'Manifestação':'manifestacao', 'Grupo de Manifestação':'grupo_manifestacao', 'Tipo de Manifestação':'tipo_manifestacao', 'Login do Agente':'login_agente', 'Solução Cliente':'solucao_cliente', 'CSat':'CSat', 'Nota':'nota', 'Nota_Anterior':'nota_anterior', 'Comentário':'comentario', 'Responsável':'responsavel', 'Deadline':'dedline', 'Categoria':'categoria', 'Status':'status', 'Prioridade':'prioridade'}, inplace=True)
        df["ARQUIVO"] = blob.name
        df["DATA_UPLOAD"] = blob.time_created
        df["DATA_CARGA"] = data_carga

        job = bigquery_client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )  # Make an API request.
        job.result()

carga_xlsx(0) 



