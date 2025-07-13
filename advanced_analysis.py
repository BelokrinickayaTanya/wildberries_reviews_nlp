%pip install pgvector psycopg2-binary
import psycopg2
import numpy as np
from pgvector.psycopg2 import register_vector
import pandas as pd
from datetime import datetime



def find_similar_reviews(conn, review_id, limit=5, similarity_threshold=0.7):
    """
    Поиск похожих отзывов по векторному представлению
    """
    try:
        # Получаем вектор целевого отзыва
        target_embedding = get_review_embedding(conn, review_id)
        if target_embedding is None:
            print(f"Отзыв с ID {review_id} не найден")
            return pd.DataFrame()

        cursor = conn.cursor()
        cursor.execute("""
            SELECT r.review_id, r.review_text, r.rating,
                   p.product_name, p.product_id,
                   sa.sentiment_label, sa.sentiment_score,
                   (1 - (re.embedding <#> %s) / 2) AS similarity
            FROM review_embeddings re
            JOIN reviews r ON re.review_id = r.review_id
            JOIN products p ON r.product_id = p.product_id
            JOIN sentiment_analysis sa ON r.review_id = sa.review_id
            WHERE r.review_id != %s AND (1 - (re.embedding <#> %s) / 2) >= %s
            ORDER BY similarity DESC
            LIMIT %s
        """, (target_embedding, review_id, target_embedding, similarity_threshold, limit))

        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        cursor.close()

        if not results:
            print("Похожие отзывы не найдены")
            return pd.DataFrame()

        return pd.DataFrame(results, columns=columns)

    except Exception as e:
        print(f"Ошибка при поиске похожих отзывов: {e}")
        return pd.DataFrame()
    


def main():
    """Основная функция выполнения"""
    conn = None
    try:
        conn = psycopg2.connect(**get_db_config())
        register_vector(conn)

        # Тестируем с ID = 123
        review_id = 123

        # Получение эмбеддинга отзыва
        embedding = get_review_embedding(conn, review_id)
        if embedding is not None:
            print(f"Эмбеддинг отзыва {review_id}: {embedding[:10]}...")
        else:
            print(f"Отзыв с ID {review_id} не найден или не имеет эмбеддинга")
            return

        # Поиск похожих отзывов
        similar_reviews = find_similar_reviews(conn, review_id=review_id, limit=5)
        if not similar_reviews.empty:
            print("\nНайдены похожие отзывы:")
            print(similar_reviews)
        else:
            print("\nПохожие отзывы не найдены")

    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()    

