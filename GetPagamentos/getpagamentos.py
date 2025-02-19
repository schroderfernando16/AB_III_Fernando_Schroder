import os
import json
import pymysql
import boto3
from decimal import Decimal

# Configuração do Secrets Manager
secrets_client = boto3.client('secretsmanager', region_name=os.getenv("REGION_NAME"))
SECRET_ARN = os.getenv("SECRET_ARN")
proxy = os.getenv("DB_PROXY")
dbname = os.getenv("DB_NAME")

def get_db_credentials():
    """
    Busca as credenciais do banco de dados no AWS Secrets Manager.
    """
    try:
        print(f"🔍 Buscando credenciais no Secrets Manager ({SECRET_ARN})...")
        response = secrets_client.get_secret_value(SecretId=SECRET_ARN)
        secret = json.loads(response["SecretString"])

        return {
            "host": proxy,
            "user": secret["username"],
            "password": secret["password"],
            "database": dbname,
        }
    except Exception as e:
        print(f"❌ Erro ao buscar credenciais do Secrets Manager: {e}")
        return None

def convert_decimal_fields(rows):
    """
    Converte todos os campos do tipo Decimal para float em uma lista de dicionários.
    """
    for row in rows:
        for key, value in row.items():
            if isinstance(value, Decimal):
                row[key] = float(value)
    return rows

def lambda_handler(event, context):
    """
    Função Lambda para buscar todas as conexões de um aluno.
    """
    try:
        print(f"📌 Evento recebido: {event}")

        # Tratamento de requisição OPTIONS para CORS
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

        # Obtendo parâmetros da query string
        params = event.get("queryStringParameters", {}) or {}
        id_aluno = params.get("id_aluno")

        if not id_aluno:
            return {
                "statusCode": 400,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"error": "O parâmetro 'id_aluno' é obrigatório."})
            }

        print(f"✅ Buscando conexões para o aluno ID: {id_aluno}")

        # Obtém credenciais seguras
        creds = get_db_credentials()
        if not creds:
            return {
                "statusCode": 500,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"error": "Falha ao obter credenciais do banco"})
            }

        # Conectando ao banco via RDS Proxy
        conn = pymysql.connect(
            host=creds["host"],
            user=creds["user"],
            password=creds["password"],
            database=creds["database"],
            connect_timeout=10
        )

        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            # Query SQL para buscar conexões do aluno
            sql = """
            SELECT C.id_conexao, P.nome AS professor, M.nome_materia, C.horas_contratadas, C.status
            FROM Conexoes_Aluno_Prof C
            JOIN Professores P ON C.id_professor = P.id_professor
            JOIN Materias M ON C.id_materia = M.id_materia
            WHERE C.id_aluno = %s
            """
            
            cursor.execute(sql, (id_aluno,))
            conexoes = cursor.fetchall()

        # Converte os campos Decimal para float antes de serializar JSON
        conexoes_formatadas = convert_decimal_fields(conexoes)

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS, GET",
                "Access-Control-Allow-Headers": "Content-Type",
                "Content-Type": "application/json"
            },
            "body": json.dumps(conexoes_formatadas)
        }

    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
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
