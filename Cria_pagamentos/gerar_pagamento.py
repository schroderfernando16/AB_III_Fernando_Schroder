import os
import json
import pymysql
import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

# 🔧 Inicializa os componentes do AWS Powertools
logger = Logger(service="processamento_pagamentos")
metrics = Metrics(namespace="AplicacaoEducacional", service="consultas_no_banco")  # ✅ Namespace padronizado
tracer = Tracer(service="processamento_pagamentos")  # 🔍 Habilita o tracing

# 🔒 Configuração do Secrets Manager e SQS
secrets_client = boto3.client('secretsmanager', region_name=os.getenv("REGION_NAME"))
sqs = boto3.client('sqs', region_name=os.getenv("REGION_NAME"))

# 🔧 Variáveis de ambiente da AWS Lambda
SECRET_ARN = os.getenv("SECRET_ARN")
DB_PROXY = os.getenv("DB_PROXY")
DB_NAME = os.getenv("DB_NAME")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")

@tracer.capture_method
def get_db_credentials():
    """
    Busca as credenciais do banco de dados no AWS Secrets Manager.
    """
    try:
        logger.info(f"Buscando credenciais no Secrets Manager ({SECRET_ARN})...")
        response = secrets_client.get_secret_value(SecretId=SECRET_ARN)
        secret = json.loads(response["SecretString"])
        return {
            "host": DB_PROXY,
            "user": secret["username"],
            "password": secret["password"],
            "database": DB_NAME,
        }
    except Exception as e:
        logger.error(f"Erro ao buscar credenciais do Secrets Manager: {e}")
        raise  # Propaga erro para análise no CloudWatch e X-Ray

@tracer.capture_lambda_handler  # 🔍 Captura automaticamente a execução da Lambda
@logger.inject_lambda_context
@metrics.log_metrics
def lambda_handler(event, context):
    """
    Função Lambda para registrar um pagamento no banco via RDS Proxy e enviar para SQS.
    """
    try:
        logger.info("Evento recebido", extra={"event": event})
        tracer.put_annotation("Function", "GerarPagamento")  # Adiciona anotações no X-Ray

        # ✅ Tratamento de requisição OPTIONS para CORS
        if event.get("httpMethod") == "OPTIONS":
            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "OPTIONS, GET, POST",
                    "Access-Control-Allow-Headers": "Content-Type",
                },
                "body": json.dumps({"message": "CORS OK!"})
            }

        # 🔍 Verifica se há um corpo na requisição
        if "body" not in event or not event["body"]:
            return cors_response(400, {"error": "Corpo da requisição está vazio."})

        body = json.loads(event["body"])
        id_conexao = body.get("id_conexao")
        valor = body.get("valor")
        forma_pagamento = body.get("forma_pagamento")

        if not all([id_conexao, valor, forma_pagamento]):
            return cors_response(400, {"error": "Todos os campos são obrigatórios."})

        creds = get_db_credentials()
        if not creds:
            return cors_response(500, {"error": "Falha ao obter credenciais do banco"})

        pagamento_id = processar_pagamento(creds, id_conexao, valor, forma_pagamento)

        # ✅ Registra a métrica personalizada no CloudWatch
        metrics.add_metric(name="PagamentosRegistrados", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="InsertsNoBanco", unit=MetricUnit.Count, value=1)  # 📊 Nova métrica para INSERTs no banco

        return cors_response(201, {
            "message": "Pagamento registrado e enviado para processamento!",
            "id_pagamento": pagamento_id
        })

    except Exception as e:
        logger.exception("Erro inesperado")
        tracer.put_annotation("Error", str(e))  # Log de erro no X-Ray
        return cors_response(500, {"error": str(e)})

@tracer.capture_method  # 🔍 Adiciona tracing detalhado para esta função
def processar_pagamento(creds, id_conexao, valor, forma_pagamento):
    """
    Processa o pagamento e envia mensagem para o SQS.
    """
    try:
        conn = pymysql.connect(
            host=creds["host"],
            user=creds["user"],
            password=creds["password"],
            database=creds["database"],
            connect_timeout=10,
            cursorclass=pymysql.cursors.DictCursor
        )

        with conn.cursor() as cursor:
            sql = """
            INSERT INTO Pagamentos (id_conexao, valor, forma_pagamento, status_pagamento)
            VALUES (%s, %s, %s, 'Pendente')
            """
            cursor.execute(sql, (id_conexao, valor, forma_pagamento))
            conn.commit()
            pagamento_id = cursor.lastrowid

        logger.info(f"✅ Pagamento inserido com ID: {pagamento_id}")

        mensagem_sqs = {
            "id_pagamento": pagamento_id,
            "id_conexao": id_conexao,
            "valor": valor,
            "forma_pagamento": forma_pagamento,
            "status_pagamento": "Pendente"
        }

        logger.info(f"📩 Enviando mensagem para SQS: {mensagem_sqs}")

        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(mensagem_sqs)
        )

        return pagamento_id

    except Exception as e:
        logger.error(f"❌ Erro ao processar pagamento: {e}")
        raise

def cors_response(status_code, body):
    """
    Gera uma resposta com CORS para a API Gateway.
    """
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS, GET, POST",
            "Access-Control-Allow-Headers": "Content-Type",
        },
        "body": json.dumps(body)
    }
