%pip install pymorphy3
import psycopg2
from psycopg2.extras import execute_values
from sentence_transformers import SentenceTransformer
import string
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from collections import defaultdict
import json
from pymorphy3 import MorphAnalyzer
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
russian_stopwords = stopwords.words('russian')
from dotenv import load_dotenv
import os
import re


# инициализация модели эмбеддингов SentenceTransformer (модель берём готовую)
model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
batch_size = 100  # оптимальный размер батча для обработки и скорости



morph = MorphAnalyzer()

def preprocess_text(text):
    """Улучшенная предобработка текста с сохранением отрицаний"""
    text = text.lower()  # приводим текст к нижнему регистру
    text = re.sub(r'[^а-яА-Яa-zA-Z\s]', '', text)  # удаляем пунктуацию и цифры
    
    # Сохраняем отрицания ("не" и "нет") перед удалением стоп-слов
    negations = {'не', 'нет'}
    tokens = word_tokenize(text)
    
    russian_stop = set(stopwords.words('russian') + 
                      list(string.punctuation) + 
                      ['это', 'весь', 'который', 'такой', 'свой'])
    
    # Удаляем стоп-слова, но оставляем отрицания
    filtered_tokens = []
    for token in tokens:
        if (token not in russian_stop or token in negations) and len(token) >= 2 and token.isalpha():
            # Нормализуем слово
            parsed = morph.parse(token)[0]
            normalized = parsed.normal_form
            
            # Убираем служебные части речи, но оставляем отрицания
            if parsed.tag.POS not in {'CONJ', 'PRCL', 'NPRO'} or token in negations:
                filtered_tokens.append(normalized)
    
    return filtered_tokens



def process_batch(conn, batch):
    """
    Обрабатывает один батч записей из таблицы и сохраняет текстовые эмбеддинги в базе
    """
    try:
        # Подготавливаем тексты для обработки
        processed_reviews = []
        valid_rows = []
        
        for row in batch:
            if not row or len(row) < 2:
                continue
                
            review_id = row[0]
            text = row[1] if row[1] else ''
            
            try:
                # preprocess_text возвращает список токенов, объединяем их в строку
                tokens = preprocess_text(text)
                if tokens:  # Если есть хотя бы один токен
                    processed_reviews.append(" ".join(tokens))
                    valid_rows.append(row)
            except Exception as e:
                print(f"Error preprocessing review {review_id}: {e}")
                continue
        
        if not processed_reviews:
            print("No valid texts in batch after preprocessing")
            return
            
        # Получаем эмбеддинги
        try:
            embeddings = model.encode(processed_reviews, show_progress_bar=False)
        except Exception as e:
            print(f"Error generating embeddings: {e}")
            raise
            
        # Подготавливаем данные для вставки
        insert_data = []
        for i, embedding in enumerate(embeddings):
            try:
                insert_data.append((
                    valid_rows[i][0],  # review_id
                    embedding.tolist(),
                    'paraphrase-multilingual-MiniLM-L12-v2'
                ))
            except Exception as e:
                print(f"Error preparing embedding for review {valid_rows[i][0]}: {e}")
                continue
        
        if not insert_data:
            print("No valid embeddings to insert")
            return
            
        # Вставка в базу
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO review_embeddings (review_id, embedding, model_version)
                VALUES %s
                ON CONFLICT (review_id) 
                DO UPDATE SET 
                    embedding = EXCLUDED.embedding,
                    model_version = EXCLUDED.model_version,
                    updated_at = CURRENT_TIMESTAMP
                """,
                insert_data,
                template="(%s, %s::vector, %s)",
                page_size=len(insert_data)
            )
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        print(f"Error processing batch: {e}")
        raise


def main():

    conn = None
    try:

        load_dotenv(r"C:\Users\Татьяна\Desktop\Учеба\Продвинутые запросы SQL\Итоговый проект\.env.txt")  # загружает переменные из .env

        conn = psycopg2.connect(**get_db_config())
        conn.autocommit = False  # включаем явное управление транзакциями (контролируем commit)

        with conn.cursor() as cur:
            # подсчитываем количество записей, которые ещё нужно обработать 
            cur.execute("""SELECT COUNT(*) FROM reviews r LEFT JOIN review_embeddings re ON r.review_id = re.review_id
                WHERE re.review_id IS NULL AND r.review_text IS NOT NULL""")
            total_count = cur.fetchone()[0]
            print(f"Total records to process: {total_count}")

            # 
            offset = 0
            processed = 0

            while True:
                
                cur.execute(
                    """
                    SELECT r.review_id, r.review_text 
                    FROM reviews r
                    LEFT JOIN review_embeddings re ON r.review_id = re.review_id
                    WHERE re.review_id IS NULL 
                    AND r.review_text IS NOT NULL
                    ORDER BY r.review_id 
                    LIMIT %s OFFSET %s """,
                    (batch_size, offset)
                )
                batch = cur.fetchall()
                if not batch:
                    break  # если батч пустой — завершаем основной цикл

                print(f"Processing batch {offset//batch_size + 1}...")  # информируем о прогрессе
                process_batch(conn, batch)  # вызываем функцию для обработки батча 
                offset += batch_size  # увеличиваем смещение для следующей порции
                processed += len(batch)

    except Exception as e:
        # если возникла ошибка на любом этапе, выводим сообщение
        print(f"Fatal error: {e}")
    finally:
        # в любом случае закрываем соединение, если оно открыто
        if conn and not conn.closed:
            conn.close()
        print("Processing completed.")    

if __name__ == "__main__":
    main()

