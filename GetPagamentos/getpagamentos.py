import os
import json
import pymysql
import boto3
from decimal import Decimal
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

# 🔹 Inicializa Powertools (Logger, Métricas, Tracing)
logger = Logger(service="consultas_no_banco")
metrics = Metrics(namespace="AplicacaoEducacional", service="consultas_no_banco")  # ✅ Namespace padronizado
tracer = Tracer(service="consultas_no_banco")

# 🔹 Configuração do Secrets Manager e RDS Proxy
secrets_client = boto3.client('secretsmanager', region_name=os.getenv("REGION_NAME"))

# 🔹 Variáveis de ambiente
SECRET_ARN = os.getenv("SECRET_ARN")  # Secret Manager do banco
DB_PROXY = os.getenv("DB_PROXY")  # Endpoint do RDS Proxy
DB_NAME = os.getenv("DB_NAME")  # Nome do banco

@tracer.capture_method
def get_db_credentials():
    """
    Obtém credenciais do banco via AWS Secrets Manager.
    """
    try:
        logger.info(f"Buscando credenciais no Secrets Manager: {SECRET_ARN}")
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
        raise

def convert_decimal_fields(rows):
    """
    Converte todos os campos Decimal para float antes de serializar em JSON.
    """
    for row in rows:
        for key, value in row.items():
            if isinstance(value, Decimal):
                row[key] = float(value)
    return rows

@tracer.capture_lambda_handler
@logger.inject_lambda_context
@metrics.log_metrics
def lambda_handler(event, context):
    """
    Função Lambda para buscar todos os pagamentos de um aluno específico.
    """
    try:
        logger.info("Evento recebido", extra={"event": event})
        tracer.put_annotation("Function", "GetAlunoPagamentos")  # 🔍 Adiciona anotação no X-Ray

        # 🔹 Tratamento CORS para requisição OPTIONS
        if event.get("httpMethod") == "OPTIONS":
            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "OPTIONS, GET",
                    "Access-Control-Allow-Headers": "Content-Type",
                },
                "body": json.dumps({"message": "CORS OK!"})
            }

        # 🔹 Obtém parâmetros da query string
        params = event.get("queryStringParameters", {}) or {}
        id_aluno = params.get("id_aluno")

        if not id_aluno:
            return {
                "statusCode": 400,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"error": "O parâmetro 'id_aluno' é obrigatório."})
            }

        logger.info(f"Buscando pagamentos para o aluno ID: {id_aluno}")

        # 🔹 Obtém credenciais seguras
        creds = get_db_credentials()

        # 🔹 Consulta pagamentos do aluno no banco de dados
        pagamentos = consultar_pagamentos_aluno(creds, id_aluno)

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS, GET",
                "Access-Control-Allow-Headers": "Content-Type",
                "Content-Type": "application/json"
            },
            "body": json.dumps(pagamentos)
        }

    except Exception as e:
        logger.exception("Erro inesperado")
        tracer.put_annotation("Error", str(e))  # 🔍 Adiciona erro ao X-Ray
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS, GET",
                "Access-Control-Allow-Headers": "Content-Type",
                "Content-Type": "application/json"
            },
            "body": json.dumps({"error": str(e)})
        }

@tracer.capture_method
def consultar_pagamentos_aluno(creds, id_aluno):
    """
    Executa a consulta SQL para buscar pagamentos do aluno no banco de dados.
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
            SELECT P.id_pagamento, C.id_conexao, P.valor, P.forma_pagamento, P.status_pagamento
            FROM Pagamentos P
            JOIN Conexoes_Aluno_Prof C ON P.id_conexao = C.id_conexao
            WHERE C.id_aluno = %s
            """
            logger.info(f"Executando SQL: {sql} com id_aluno={id_aluno}")
            cursor.execute(sql, (id_aluno,))
            pagamentos = cursor.fetchall()

            # 🔥 Adiciona métrica unificada de leitura no banco
            metrics.add_metric(name="LeituraNoBanco", unit=MetricUnit.Count, value=1)

        logger.info(f"Pagamentos encontrados para aluno {id_aluno}: {pagamentos}")

        return convert_decimal_fields(pagamentos)

    except Exception as e:
        logger.error(f"Erro ao buscar pagamentos no banco: {e}")
        raise
