%pip install psycopg2
import psycopg2
from datetime import datetime
from psycopg2.extras import execute_batch
import numpy as np
import os

def safe_json_parse(json_str):
    try:
        return json.loads(json_str.replace("'", '"')) if pd.notna(json_str) else []
    except:
        return []

def clean_int(value):
    try:
        return int(float(value)) if pd.notna(value) else None
    except:
        return None

def clean_str(value, max_len=None):
    try:
        if pd.isna(value) or value == '':
            return None
        s = str(value).strip()
        return s[:max_len] if max_len else s
    except:
        return None

def clean_float(value):
    try:
        return float(value) if pd.notna(value) else None
    except:
        return None

def clean_date(value):
    try:
        return pd.to_datetime(value, errors='coerce') if pd.notna(value) else None
    except:
        return None
    
def clean_bool(value, default=False):
    try:
        if isinstance(value, (bool, np.bool_)):
            return bool(value)
    except:
        return None       
    
def load_products(conn):
    try:
        """Загрузка товаров"""
        copy_df = pd.read_csv('prepared_data.csv', low_memory=False)
        print(f"Прочитано строк из CSV: {len(copy_df)}")

     
        valid_products = {}
        skipped = 0

        for _, row in copy_df.iterrows():
            try:
            # Все поля преобразуются к простым типам 
                product_name = clean_str(row['name'])
            
            # Обязательные поля
                if not product_name:
                    skipped += 1
                    continue

            # Генерация product_id
                product_id = clean_int(row.get('product_id')) or generate_new_id(conn)    
            
            # Остальные поля с проверкой
                description = clean_str(row['description']) or ''
                has_sizes = clean_bool(row['has_sizes'])
                color = clean_str(row['color']) or ''
            
            # Получение временных меток 
                created_at = clean_date(row.get('created_at')) or datetime.now()
                updated_at = clean_date(row.get('updated_at')) or datetime.now()
            
            # Контролируемая денормализация:
       
                valid_products[product_id] = (
                    product_id,
                    product_name,
                    description,
                    has_sizes,
                    color,
                    created_at,
                    updated_at
                
                )
            except Exception as e:
                print(f"Ошибка обработки товара: {e}")
                skipped

            print(f"Валидных товаров: {len(valid_products)}")
            print(f"Пропущено строк: {skipped}")    
    
    # Вставка в БД
        with conn.cursor() as cur:
        
            execute_batch(cur, """
                INSERT INTO products (product_id, product_name, description, has_sizes, color, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (product_id) DO NOTHING
                """, list(valid_products.values()))

        
        conn.commit()
        print(f"Добавлено {len(valid_products)} товаров")
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при загрузке в БД: {e}")
        raise


def generate_new_id(conn):
    """Генерируем ID через последовательность в БД."""
    with conn.cursor() as cur:
        cur.execute("SELECT nextval('products_product_id_seq')")
        return cur.fetchone()[0]





def load_users(conn):
    try:
        """Загрузка и нормализация пользователей"""
        copy_df = pd.read_csv('prepared_data.csv', low_memory=False)
        valid_users = {} # таблица users
        skipped = 0

         
        for _, row in copy_df.iterrows():
            try:
                user_name = clean_str(row['reviewerName'])
            
                if not user_name:
                    skipped += 1
                    continue

            # Генерация user_id
                user_id = clean_int(row.get('user_id')) or generate_new_id(conn)    
            
            # Остальные поля с проверкой
                gender = clean_str(row['gender_token']) or None
            
            
            # Получение временных меток 
                created_at = clean_date(row.get('created_at')) or datetime.now()
                updated_at = clean_date(row.get('updated_at')) or datetime.now()
            
            # Контролируемая денормализация:
       
                valid_users[user_id] = (
                    user_id,
                    user_name,
                    gender,
                    created_at,
                    updated_at
                
                )
                
            except Exception as e:
                print(f"Ошибка обработки пользователя: {e}")
                skipped += 1

        print(f"Валидных пользователей: {len(valid_users)}")
        print(f"Пропущено строк: {skipped}")    

    # вставка пользователей
        with conn.cursor() as cur:
            execute_batch(cur, """
                INSERT INTO users (user_id, user_name, gender, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING
                """,list(valid_users.values()))

        
            conn.commit()
            print(f"Добавлено {len(valid_users)} пользователей")

    except Exception as e:
        conn.rollback()
        print(f"Ошибка при загрузке в БД: {e}")
        raise

def generate_new_id(conn):
    """Генерируем ID через последовательность в БД."""
    with conn.cursor() as cur:
        cur.execute("SELECT nextval('users_user_id_seq')")
        return cur.fetchone()[0]   
 


def load_reviews(conn):

    """
    Загружает данные отзывов из CSV-файла в таблицу reviews.
    Предполагается, что файл имеет следующие колонки:
    user_name, product_name, review_text, rating, matching_size
    
    Функция находит соответствующие user_id и product_id в таблицах users и products
    перед вставкой в таблицу reviews.
    """
    cursor = conn.cursor()
    
    try:
        copy_df = pd.read_csv('prepared_data.csv', low_memory=False)
        print(f"Прочитано строк из CSV: {len(copy_df)}")

        inserted = 0
        skipped = 0
            
        for index, row in copy_df.iterrows():
            try:

                # Получаем user_id по имени пользователя
                cursor.execute(
                    "SELECT user_id FROM users WHERE user_name = %s",
                    [row['reviewerName']]
                )
                user_id = cursor.fetchone()
                if not user_id:
                    print(f"Пользователь {row['reviewerName']} не найден, пропускаем отзыв")
                    skipped += 1
                    continue
                user_id = user_id[0]
                
                # Получаем product_id по названию продукта
                cursor.execute(
                    "SELECT product_id FROM products WHERE product_name = %s",
                    [row['name']]
                )
                product_id = cursor.fetchone()
                if not product_id:
                    print(f"Продукт {row['name']} не найден, пропускаем отзыв")

                    skipped += 1
                    continue
                product_id = product_id[0]
                
        # Вставляем отзыв
                cursor.execute(
                    """
                    INSERT INTO reviews 
                    (user_id, product_id, review_text, rating, matching_size)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    [
                        user_id,
                        product_id,
                        row['text'],
                        int(row['mark']),
                        row['matchingSize'] 
                    ]
                )
                inserted += 1

            except Exception as e:
               print(f"Ошибка при обработке строки {index}: {e}")
               skipped += 1
               continue 
        
        conn.commit()
        print("Данные отзывов успешно загружены")
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при загрузке отзывов: {e}")
    finally:
        cursor.close()


def main():
    try:
        with psycopg2.connect(**get_db_config()) as conn:
            print("Начало загрузки данных...")


            # Проверяем текущее состояние БД
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM products")
                print(f"Товаров в БД до вставки: {cur.fetchone()[0]}")
                
                cur.execute("SELECT COUNT(*) FROM users")
                print(f"Пользователей в БД до вставки: {cur.fetchone()[0]}")
                
                cur.execute("SELECT COUNT(*) FROM reviews")
                print(f"Отзывов в БД до вставки: {cur.fetchone()[0]}")

            # загрузка по порядку
            load_products(conn)
            load_users(conn)
            load_reviews(conn)

            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM products")
                print(f"Товаров в БД после вставки: {cur.fetchone()[0]}")
                
                cur.execute("SELECT COUNT(*) FROM users")
                print(f"Пользователей в БД после вставки: {cur.fetchone()[0]}")
                
                cur.execute("SELECT COUNT(*) FROM reviews")
                print(f"Отзывов в БД после вставки: {cur.fetchone()[0]}")
           
            
            print("Все данные успешно загружены!")
            
    except Exception as e:
        print(f"Ошибка: {e}")
        raise

if __name__ == "__main__":
    main()            
        
       