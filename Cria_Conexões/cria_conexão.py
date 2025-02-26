import os
import json
import pymysql
import boto3
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

# 🔹 Inicializa Powertools (Logger, Métricas, Tracing)
logger = Logger(service="criar_conexao")
metrics = Metrics(namespace="AplicacaoEducacional", service="criar_conexao")
tracer = Tracer(service="criar_conexao")

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
    Função Lambda para criar uma conexão entre aluno e professor via RDS Proxy.
    """
    try:
        logger.info("Evento recebido", extra={"event": event})
        tracer.put_annotation("Function", "CriarConexao")  # 🔍 Adiciona anotação no X-Ray

        # 🔹 Tratamento CORS para requisição OPTIONS
        if event.get("httpMethod") == "OPTIONS":
            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "OPTIONS, POST, GET",
                    "Access-Control-Allow-Headers": "Content-Type",
                },
                "body": json.dumps({"message": "CORS OK!"})
            }

        # 🔹 Verifica se há um corpo na requisição
        if "body" not in event or not event["body"]:
            return {
                "statusCode": 400,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"error": "Corpo da requisição está vazio."})
            }

        # 🔹 Decodifica o JSON recebido
        body = json.loads(event["body"])

        id_professor = body.get("id_professor")
        id_aluno = body.get("id_aluno")
        id_materia = body.get("id_materia")
        horas_contratadas = body.get("horas_contratadas")

        # 🔹 Valida se todos os campos foram fornecidos
        if not all([id_professor, id_aluno, id_materia, horas_contratadas]):
            return {
                "statusCode": 400,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"error": "Todos os campos são obrigatórios."})
            }

        id_professor = int(id_professor)
        id_aluno = id_aluno
        id_materia = int(id_materia)
        horas_contratadas = int(horas_contratadas)

        # 🔹 Obtém credenciais seguras
        creds = get_db_credentials()

        # 🔹 Cria a conexão no banco
        conexao_id = criar_conexao_db(creds, id_professor, id_aluno, id_materia, horas_contratadas)

        # ✅ Registra métricas no CloudWatch
        metrics.add_metric(name="ConexoesCriadas", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="InsertsNoBanco", unit=MetricUnit.Count, value=1)  # 📊 Nova métrica para INSERTs no banco

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS, POST, GET",
                "Access-Control-Allow-Headers": "Content-Type",
            },
            "body": json.dumps({"message": "Conexão criada com sucesso!", "id_conexao": conexao_id})
        }

    except Exception as e:
        logger.exception("Erro inesperado")
        tracer.put_annotation("Error", str(e))  # 🔍 Log de erro no X-Ray
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS, POST, GET",
                "Access-Control-Allow-Headers": "Content-Type",
            },
            "body": json.dumps({"error": str(e)})
        }

@tracer.capture_method
def criar_conexao_db(creds, id_professor, id_aluno, id_materia, horas_contratadas):
    """
    Insere a conexão na tabela Conexoes_Aluno_Prof no RDS Proxy.
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
            INSERT INTO Conexoes_Aluno_Prof (id_professor, id_aluno, id_materia, horas_contratadas, status)
            VALUES (%s, %s, %s, %s, 'Ativo')
            """
            cursor.execute(sql, (id_professor, id_aluno, id_materia, horas_contratadas))
            conn.commit()
            conexao_id = cursor.lastrowid

        logger.info(f"✅ Conexão criada com ID: {conexao_id}")
        return conexao_id

    except Exception as e:
        logger.error(f"Erro ao criar conexão: {e}")
        raise
