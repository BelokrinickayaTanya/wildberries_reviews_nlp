import psycopg2
from dotenv import load_dotenv
import os

def get_db_config():
    return {
        'host': os.getenv('DB_HOST'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT', '5432')
    }

def test_connection():
    try:
        conn = psycopg2.connect(**get_db_config())
        print("✔ Подключение к базе данных успешно")
        return True
    except psycopg2.OperationalError as e:
        print(f"✖ Ошибка подключения: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

if not test_connection():
    exit(1)