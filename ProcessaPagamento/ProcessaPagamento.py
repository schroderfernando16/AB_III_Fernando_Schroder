import os
import json
import random
import pymysql
import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

# 🔧 Inicializa AWS Powertools
logger = Logger(service="processamento_pagamentos")
metrics = Metrics(namespace="AplicacaoEducacional", service="consultas_no_banco")  # ✅ Namespace padronizado
tracer = Tracer(service="processamento_pagamentos")

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
    """Busca as credenciais do banco de dados no AWS Secrets Manager."""
    try:
        logger.info(f"🔍 Buscando credenciais no Secrets Manager ({SECRET_ARN})...")
        response = secrets_client.get_secret_value(SecretId=SECRET_ARN)
        secret = json.loads(response["SecretString"])
        creds = {
            "host": DB_PROXY,
            "user": secret["username"],
            "password": secret["password"],
            "database": DB_NAME,
        }
        logger.info("✅ Credenciais obtidas com sucesso.")
        return creds
    except Exception as e:
        logger.error(f"❌ Erro ao buscar credenciais do Secrets Manager: {e}")
        raise

@tracer.capture_method
def processar_pagamento(pagamento):
    """Simula a integração com o PayPal e define um status aleatório para o pagamento."""
    logger.info(f"💳 Simulando integração com PayPal para pagamento: {pagamento}")
    status_pagamento = random.choice(["Pago", "Cancelado"])
    logger.info(f"📌 Resultado da simulação: {status_pagamento}")
    return status_pagamento

@tracer.capture_method
def atualizar_status_pagamento(conn, id_pagamento, status_pagamento):
    """Atualiza o status do pagamento no banco de dados."""
    try:
        with conn.cursor() as cursor:
            sql = """
            UPDATE Pagamentos
            SET status_pagamento = %s
            WHERE id_pagamento = %s
            """
            cursor.execute(sql, (status_pagamento, id_pagamento))
            conn.commit()
            logger.info(f"✅ Status atualizado para '{status_pagamento}' no pagamento {id_pagamento}")
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar status do pagamento: {e}")
        raise

@tracer.capture_lambda_handler
@logger.inject_lambda_context
@metrics.log_metrics
def lambda_handler(event, context):
    """Função Lambda para processar pagamentos recebidos do SQS."""
    try:
        logger.info("📌 Evento recebido", extra={"event": event})

        # Obtém credenciais do banco
        creds = get_db_credentials()
        if not creds:
            return cors_response(500, {"error": "Falha ao obter credenciais do banco"})

        # Conectando ao banco via RDS Proxy
        try:
            conn = pymysql.connect(
                host=creds["host"],
                user=creds["user"],
                password=creds["password"],
                database=creds["database"],
                connect_timeout=10,
                cursorclass=pymysql.cursors.DictCursor
            )
            logger.info("✅ Conexão com o banco bem-sucedida!")
        except Exception as e:
            logger.error(f"❌ Erro ao conectar ao banco: {e}")
            return cors_response(500, {"error": "Falha ao conectar ao banco"})

        # Processar mensagens recebidas via trigger do SQS
        if "Records" not in event:
            logger.info("📭 Nenhuma mensagem recebida via SQS.")
            return cors_response(200, {"message": "Nenhuma solicitação de pagamento encontrada."})

        for record in event["Records"]:
            try:
                # Decodifica a mensagem corretamente
                mensagem = json.loads(record["body"])
                logger.info(f"🔄 Processando pagamento ID: {mensagem['id_pagamento']}")

                # Simular a integração com PayPal
                status_pagamento = processar_pagamento(mensagem)

                # Atualizar o status do pagamento no banco
                atualizar_status_pagamento(conn, mensagem["id_pagamento"], status_pagamento)

                # ✅ Enviar métricas para CloudWatch
                metrics.add_metric(name="UpdateNoBanco", unit=MetricUnit.Count, value=1)
                if status_pagamento == "Pago":
                    metrics.add_metric(name="PagamentosConcluidos", unit=MetricUnit.Count, value=1)
                elif status_pagamento == "Cancelado":
                    metrics.add_metric(name="PagamentosCancelados", unit=MetricUnit.Count, value=1)

            except Exception as e:
                logger.error(f"❌ Erro ao processar a mensagem do SQS: {e}")

        return cors_response(200, {"message": "Pagamentos processados com sucesso."})

    except Exception as e:
        logger.error(f"❌ Erro inesperado: {e}")
        return cors_response(500, {"error": str(e)})

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
