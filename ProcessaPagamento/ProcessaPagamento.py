import os
import json
import pymysql
import boto3

# Configuração do Secrets Manager e SQS
secrets_client = boto3.client('secretsmanager', region_name=os.getenv("REGION_NAME"))
sqs = boto3.client('sqs', region_name=os.getenv("REGION_NAME"))

# Variáveis de ambiente configuradas na AWS Lambda
SECRET_ARN = os.getenv("SECRET_ARN")  # Secret Manager com credenciais do banco
DB_PROXY = os.getenv("DB_PROXY")  # Endpoint do RDS Proxy
DB_NAME = os.getenv("DB_NAME")  # Nome do banco de dados
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")  # URL da fila do SQS

def get_db_credentials():
    """
    Busca as credenciais do banco de dados no AWS Secrets Manager.
    """
    try:
        print(f"🔍 Buscando credenciais no Secrets Manager ({SECRET_ARN})...")
        response = secrets_client.get_secret_value(SecretId=SECRET_ARN)
        secret = json.loads(response["SecretString"])

        return {
            "host": DB_PROXY,  # Usar o endpoint do RDS Proxy
            "user": secret["username"],
            "password": secret["password"],
            "database": DB_NAME,
        }
    except Exception as e:
        print(f"❌ Erro ao buscar credenciais do Secrets Manager: {e}")
        return None

def lambda_handler(event, context):
    """
    Função Lambda para registrar um pagamento no banco via RDS Proxy e enviar para SQS.
    """
    try:
        print(f"📌 Evento recebido: {event}")

        # Verifica se há um corpo na requisição
        if "body" not in event or not event["body"]:
            return {"statusCode": 400, "body": json.dumps({"error": "Corpo da requisição está vazio."})}

        # Decodifica o JSON recebido
        body = json.loads(event["body"])

        id_conexao = body.get("id_conexao")
        valor = body.get("valor")
        forma_pagamento = body.get("forma_pagamento")

        # Valida se todos os campos obrigatórios foram fornecidos
        if not all([id_conexao, valor, forma_pagamento]):
            return {"statusCode": 400, "body": json.dumps({"error": "Todos os campos são obrigatórios."})}

        # Obtém credenciais seguras
        creds = get_db_credentials()
        if not creds:
            return {"statusCode": 500, "body": json.dumps({"error": "Falha ao obter credenciais do banco"})}

        # Conectando ao banco via RDS Proxy
        conn = pymysql.connect(
            host=creds["host"],
            user=creds["user"],
            password=creds["password"],
            database=creds["database"],
            connect_timeout=10,
            cursorclass=pymysql.cursors.DictCursor
        )

        with conn.cursor() as cursor:
            # Query para inserir o pagamento no banco de dados com status "Pendente"
            sql = """
            INSERT INTO Pagamentos (id_conexao, valor, forma_pagamento, status_pagamento)
            VALUES (%s, %s, %s, 'Pendente')
            """
            cursor.execute(sql, (id_conexao, valor, forma_pagamento))
            conn.commit()

            # Obtém o ID do pagamento gerado
            pagamento_id = cursor.lastrowid

        print(f"✅ Pagamento inserido com ID: {pagamento_id}")

        # Criar a mensagem para a fila do SQS
        mensagem_sqs = {
            "id_pagamento": pagamento_id,
            "id_conexao": id_conexao,
            "valor": valor,
            "forma_pagamento": forma_pagamento,
            "status_pagamento": "Pendente"
        }

        print(f"📩 Enviando mensagem para SQS: {mensagem_sqs}")

        # Envia para a fila do SQS
        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(mensagem_sqs)
        )

        return {
            "statusCode": 201,
            "body": json.dumps({"message": "Pagamento registrado e enviado para processamento!", "id_pagamento": pagamento_id})
        }

    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
