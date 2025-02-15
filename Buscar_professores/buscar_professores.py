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

def decimal_converter(obj):
    """
    Converte objetos do tipo Decimal para float para evitar erro na serialização JSON.
    """
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Tipo não serializável: {type(obj)}")

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
    Função Lambda para buscar professores e suas matérias via RDS Proxy.
    """
    try:
        print(f"📌 Evento recebido: {event}")

        # Obtendo parâmetros da query string
        params = event.get("queryStringParameters", {}) or {}
        materia = params.get("materia")
        print(f"✅ Filtros recebidos → Matéria: {materia}")

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
            connect_timeout=10
        )

        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            # Query SQL para buscar professores e matérias
            sql = """
            SELECT P.id_professor, P.nome, P.valor_hora, M.nome_materia
            FROM Professores P
            JOIN Conexao_Prof_Materias CPM ON P.id_professor = CPM.id_professor
            JOIN Materias M ON CPM.id_materia = M.id_materia
            WHERE 1=1
            """
            values = []

            if materia:
                sql += " AND M.nome_materia = %s"
                values.append(materia)

            print(f"🔄 SQL Query: {sql}")
            print(f"📌 Parâmetros: {values}")

            # Executa a consulta
            cursor.execute(sql, values)
            professores = cursor.fetchall()

        # Converte os campos Decimal para float antes de serializar JSON
        professores_convertidos = convert_decimal_fields(professores)

        return {"statusCode": 200, "body": json.dumps(professores_convertidos)}

    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
