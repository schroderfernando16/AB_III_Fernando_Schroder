import os
import json
import pymysql
import boto3

# Configuração do Secrets Manager
secrets_client = boto3.client('secretsmanager', region_name=os.getenv("REGION_NAME"))

# Variáveis de ambiente configuradas na AWS Lambda
SECRET_ARN = os.getenv("SECRET_ARN")  # Secret Manager com credenciais do banco
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

def lambda_handler(event, context):
    """
    Função Lambda para criar uma conexão entre aluno e professor via RDS Proxy.
    """
    try:
        print(f"📌 Evento recebido: {event}")

        # Verifica se há um corpo na requisição
        if "body" not in event or not event["body"]:
            return {"statusCode": 400, "body": json.dumps({"error": "Corpo da requisição está vazio."})}

        # Decodifica o JSON recebido
        body = json.loads(event["body"])

        id_professor = body.get("id_professor")
        id_aluno = body.get("id_aluno")
        id_materia = body.get("id_materia")
        horas_contratadas = body.get("horas_contratadas")

        # Valida se todos os campos foram fornecidos
        if not all([id_professor, id_aluno, id_materia, horas_contratadas]):
            return {"statusCode": 400, "body": json.dumps({"error": "Todos os campos são obrigatórios."})}

        # Converte os valores corretamente
        id_professor = int(id_professor)
        id_aluno = int(id_aluno)
        id_materia = int(id_materia)
        horas_contratadas = int(horas_contratadas)

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
            # Query para inserir a conexão na tabela Conexoes_Aluno_Prof
            sql = """
            INSERT INTO Conexoes_Aluno_Prof (id_professor, id_aluno, id_materia, horas_contratadas, status)
            VALUES (%s, %s, %s, %s, 'Ativo')
            """
            cursor.execute(sql, (id_professor, id_aluno, id_materia, horas_contratadas))
            conn.commit()

            # Obtém o ID da conexão criada
            conexao_id = cursor.lastrowid

        print(f"✅ Conexão criada com sucesso! ID: {conexao_id}")

        return {
            "statusCode": 201,
            "body": json.dumps({"message": "Conexão criada com sucesso!", "id_conexao": conexao_id})
        }

    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
