import os
import json
import pymysql
import boto3
from decimal import Decimal
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

# 🔹 Inicializa Powertools (Logger, Métricas, Tracing)
logger = Logger(service="consulta_professores")
metrics = Metrics(namespace="AplicacaoEducacional", service="consultas_no_banco")  # ✅ Namespace padronizado
tracer = Tracer(service="consulta_professores")

# 🔹 Configuração do Secrets Manager
secrets_client = boto3.client('secretsmanager', region_name=os.getenv("REGION_NAME"))

# 🔹 Variáveis de ambiente
SECRET_ARN = os.getenv("SECRET_ARN")
DB_PROXY = os.getenv("DB_PROXY")
DB_NAME = os.getenv("DB_NAME")

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

@tracer.capture_lambda_handler
@logger.inject_lambda_context
@metrics.log_metrics
def lambda_handler(event, context):
    """
    Função Lambda para buscar professores e suas matérias via RDS Proxy.
    """
    try:
        logger.info("Evento recebido", extra={"event": event})
        tracer.put_annotation("Function", "ConsultaProfessores")  # 🔍 Adiciona anotação no X-Ray

        # Obtendo parâmetros da query string
        params = event.get("queryStringParameters", {}) or {}
        materia = params.get("materia")

        logger.info(f"Filtros recebidos → Matéria: {materia}")

        # 🔹 Obtém credenciais seguras
        creds = get_db_credentials()

        # 🔹 Consulta professores no banco
        professores = buscar_professores_no_banco(creds, materia)

        # ✅ Registra métrica personalizada de leitura no banco
        metrics.add_metric(name="LeituraNoBanco", unit=MetricUnit.Count, value=1)

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS, GET",
                "Access-Control-Allow-Headers": "Content-Type",
                "Content-Type": "application/json"
            },
            "body": json.dumps(professores)
        }

    except Exception as e:
        logger.exception("Erro inesperado")
        tracer.put_annotation("Error", str(e))  # 🔍 Log de erro no X-Ray
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
def buscar_professores_no_banco(creds, materia):
    """
    Executa a query no banco para buscar professores e matérias.
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
            SELECT P.id_professor, P.nome, P.valor_hora, M.nome_materia, M.id_materia
            FROM Professores P
            JOIN Conexao_Prof_Materias CPM ON P.id_professor = CPM.id_professor
            JOIN Materias M ON CPM.id_materia = M.id_materia
            WHERE 1=1
            """
            values = []

            if materia:
                sql += " AND M.nome_materia = %s"
                values.append(materia)

            logger.info(f"🔄 SQL Query: {sql}")
            logger.info(f"📌 Parâmetros: {values}")

            # Executa a consulta
            cursor.execute(sql, values)
            professores = cursor.fetchall()

        return convert_decimal_fields(professores)

    except Exception as e:
        logger.error(f"Erro ao consultar professores: {e}")
        raise

def convert_decimal_fields(rows):
    """
    Converte todos os campos do tipo Decimal para float antes de serializar JSON.
    """
    for row in rows:
        for key, value in row.items():
            if isinstance(value, Decimal):
                row[key] = float(value)
    return rows
