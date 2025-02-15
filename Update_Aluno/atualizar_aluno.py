import os
import json
import pymysql
import boto3

# Configuração do Secrets Manager
secrets_client = boto3.client('secretsmanager', region_name=os.getenv("REGION_NAME"))

# Variáveis de ambiente configuradas na AWS Lambda
SECRET_ARN = os.getenv("SECRET_ARN")  # ARN do Secrets Manager
DB_PROXY = os.getenv("DB_PROXY")  # Endpoint do RDS Proxy
DB_NAME = os.getenv("DB_NAME")  # Nome do banco de dados

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

def aluno_existe(conn, cpf):
    """Verifica se o CPF já está cadastrado no banco de dados."""
    try:
        with conn.cursor() as cursor:
            sql = "SELECT COUNT(*) FROM Alunos WHERE cpf = %s"
            cursor.execute(sql, (cpf,))
            result = cursor.fetchone()
            return result[0] > 0
    except Exception as e:
        print(f"❌ Erro ao verificar aluno: {e}")
        return False

def lambda_handler(event, context):
    try:
        print(f"📌 Evento recebido: {event}")

        # Verifica se o corpo da requisição está presente
        if "body" not in event:
            return {"statusCode": 400, "body": json.dumps({"error": "Requisição inválida, 'body' ausente"})}

        # Converte JSON para dicionário Python
        body = json.loads(event["body"]) if isinstance(event["body"], str) else event["body"]
        print(f"📌 JSON Decodificado: {body}")

        # Verifica se os campos obrigatórios existem
        if "cpf" not in body:
            return {"statusCode": 400, "body": json.dumps({"error": "Campo 'cpf' é obrigatório para atualização"})}

        cpf = body["cpf"]
        nome = body.get("nome")  # Nome pode ser opcional

        print(f"✅ Atualizando aluno com CPF={cpf}")

        # Obtém credenciais seguras do banco
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

        with conn:
            # Verifica se o aluno existe antes de tentar atualizar
            if not aluno_existe(conn, cpf):
                return {"statusCode": 404, "body": json.dumps({"error": "Aluno não encontrado"})}

            # Construir a query de atualização dinamicamente
            sql = "UPDATE Alunos SET "
            parameters = []
            fields_to_update = []

            if nome:
                fields_to_update.append("nome = %s")
                parameters.append(nome)

            sql += ", ".join(fields_to_update)
            sql += " WHERE cpf = %s"
            parameters.append(cpf)

            print(f"🔄 SQL Query: {sql}")
            print(f"📌 Parâmetros: {parameters}")

            # Executa o UPDATE no banco de dados
            with conn.cursor() as cursor:
                cursor.execute(sql, parameters)
                conn.commit()

        return {"statusCode": 200, "body": json.dumps({"message": "Dados do aluno atualizados com sucesso"})}

    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
