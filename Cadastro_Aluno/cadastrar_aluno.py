import os
import json
import pymysql
import boto3

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
            "host": proxy,  # Usar o endpoint do RDS Proxy
            "user": secret["username"],
            "password": secret["password"],
            "database": dbname,
        }
    except Exception as e:
        print(f"❌ Erro ao buscar credenciais do Secrets Manager: {e}")
        return None

def lambda_handler(event, context):
    """Recebe um JSON com nome e CPF e cadastra um novo aluno no banco via RDS Proxy."""
    try:
        print(f"📌 Evento recebido: {event}")

        # Verifica se o corpo da requisição está presente
        if "body" not in event:
            return {"statusCode": 400, "body": json.dumps({"error": "Requisição inválida, 'body' ausente"})}

        # Converte a string JSON para um dicionário Python
        body = json.loads(event["body"]) if isinstance(event["body"], str) else event["body"]
        print(f"📌 JSON Decodificado: {body}")

        # Verifica se os campos obrigatórios existem
        if "nome" not in body or "cpf" not in body:
            return {"statusCode": 400, "body": json.dumps({"error": "Campos 'nome' e 'cpf' são obrigatórios"})}

        nome = body["nome"]
        cpf = body["cpf"]
        print(f"✅ Dados extraídos: Nome={nome}, CPF={cpf}")

        # Obtém credenciais seguras
        creds = get_db_credentials()
        if not creds:
            return {"statusCode": 500, "body": json.dumps({"error": "Falha ao obter credenciais do banco"})}

        # Conectando ao RDS Proxy via pymysql
        conn = pymysql.connect(
            host=creds["host"],
            user=creds["user"],
            password=creds["password"],
            database=creds["database"],
            connect_timeout=10
        )

        with conn.cursor() as cursor:
            # Query SQL para inserir um novo aluno
            sql = "INSERT INTO Alunos (nome, cpf) VALUES (%s, %s)"
            cursor.execute(sql, (nome, cpf))
            conn.commit()

        print("✅ Aluno cadastrado com sucesso")
        return {"statusCode": 201, "body": json.dumps({"message": "Aluno cadastrado com sucesso"})}

    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
