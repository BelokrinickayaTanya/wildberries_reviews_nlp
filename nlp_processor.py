%pip install pymorphy3
import psycopg2
from psycopg2.extras import execute_values
import string
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from pymorphy3 import MorphAnalyzer
from nltk.stem import WordNetLemmatizer
from collections import defaultdict
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
russian_stopwords = stopwords.words('russian')
from dotenv import load_dotenv
import re
from collections import defaultdict
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
from psycopg2.extras import execute_batch



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




def process_keywords_batch(conn, batch_size=1000):
    """Обработка отзывов партиями и извлечение ключевых слов для каждого отзыва"""
    cur = conn.cursor()
    
    # Проверяем, есть ли необработанные отзывы
    cur.execute("""
        SELECT COUNT(*) FROM reviews 
        WHERE review_id NOT IN (SELECT review_id FROM keywords)
    """)
    total_reviews = cur.fetchone()[0]

    if total_reviews == 0:
        print("Нет отзывов для обработки - все уже обработаны!")
        return
        
    print(f"Всего отзывов для обработки: {total_reviews}")

    # Обрабатываем отзывы партиями
    for offset in range(0, total_reviews, batch_size):
        # Получаем порцию необработанных отзывов
        cur.execute("""
            SELECT review_id, review_text 
            FROM reviews 
            WHERE review_id NOT IN (SELECT review_id FROM keywords)
            ORDER BY review_id 
            LIMIT %s OFFSET %s
        """, (batch_size, offset))
        batch_reviews = cur.fetchall()

        batch_keywords = []
        for review_id, review_text in batch_reviews:
            # Обрабатываем каждый отзыв индивидуально
            cleaned_tokens = preprocess_text(review_text)
            
            # Удаляем дубликаты и ограничиваем количество ключевых слов
            unique_tokens = list(set(cleaned_tokens))
            if len(unique_tokens) > 20:
                unique_tokens = unique_tokens[:20]  # берем первые 20 уникальных слов
            
            batch_keywords.append((review_id, unique_tokens))
        
        # Пакетное сохранение ключевых слов
        execute_batch(cur,
            "INSERT INTO keywords (review_id, keywords) VALUES (%s, %s) ON CONFLICT (review_id) DO NOTHING",
            batch_keywords
        )
        
        conn.commit()
        print(f"Обработано {min(offset + batch_size, total_reviews)} из {total_reviews} отзывов")
    
    cur.close()

def analyze_reviews_sentiment(conn):
    """Анализ тональности отзывов с учетом отрицаний и модификаторов"""
    cur = conn.cursor()
    
    # Словарь тональности в требуемом формате
    sentiment_lexicon = {
        # Позитивные слова (вес от 0.5 до 1.0)
        'отличный': ('positive', 1.0),
        'превосходный': ('positive', 1.0),
        'замечательный': ('positive', 0.9),
        'хороший': ('positive', 0.8),
        'качественный': ('positive', 0.8),
        'удобный': ('positive', 0.7),
        'рекомендовать': ('positive', 0.9),
        'довольный': ('positive', 0.8),
        'радостный': ('positive', 0.7),
        'супер': ('positive', 0.9),
        'идеальный': ('positive', 1.0),
        'восхитительный': ('positive', 1.0),
        'любить': ('positive', 0.9),
        'восторг': ('positive', 1.0),
        'прекрасный': ('positive', 0.9),
        
        # Негативные слова (вес от -0.5 до -1.0)
        'плохой': ('negative', -1.0),
        'ужасный': ('negative', -1.0),
        'кошмарный': ('negative', -1.0),
        'разочарование': ('negative', -0.9),
        'неудобный': ('negative', -0.7),
        'бракованный': ('negative', -1.0),
        'некачественный': ('negative', -0.9),
        'отвратительный': ('negative', -1.0),
        'недовольный': ('negative', -0.8),
        'ужасно': ('negative', -1.0),
        'отвратно': ('negative', -1.0),
        'недостаток': ('negative', -0.6),
        'проблема': ('negative', -0.7),
        'жаловаться': ('negative', -0.8),
        'недоработка': ('negative', -0.7),
        
        # Нейтральные слова (вес от -0.2 до 0.2)
        'обычный': ('neutral', 0.1),
        'стандартный': ('neutral', 0.1),
        'средний': ('neutral', 0.0),
        'нормальный': ('neutral', 0.2),
        'приемлемый': ('neutral', 0.2),
        'типичный': ('neutral', 0.0),
        'ожидаемый': ('neutral', 0.1),
        'удовлетворительный': ('neutral', 0.3),
        'неплохой': ('neutral', 0.4)
    }

    # Модификаторы интенсивности
    intensifiers = {
        'очень': 1.3, 'крайне': 1.5, 'совершенно': 1.4,
        'абсолютно': 1.5, 'невероятно': 1.4, 'слишком': 1.2,
        'чрезвычайно': 1.5, 'довольно': 1.2, 'особенно': 1.3,
        'немного': 0.7, 'слегка': 0.6, 'чуть': 0.5
    }

    # Отрицания
    negations = {'не', 'нет', 'ни'}

    # Получаем отзывы с ключевыми словами и рейтингом
    cur.execute("""
        SELECT r.review_id, k.keywords, r.rating 
        FROM reviews r
        JOIN keywords k ON r.review_id = k.review_id
        LEFT JOIN sentiment_analysis sa ON r.review_id = sa.review_id
        WHERE sa.review_id IS NULL OR sa.created_at < r.updated_at
    """)
    
    for review_id, keywords, rating in cur.fetchall():
        if not keywords:
            continue

        total_score = 0.0
        matched_terms = 0
        prev_word = None
        negation_active = False
        
        for word in keywords:
            if not isinstance(word, str):
                continue

            current_score = 0.0
            current_label = 'neutral'
            word_matched = False
            
            # Обработка отрицаний
            if word in negations:
                negation_active = True
                prev_word = word
                continue
                
            # Поиск в словаре тональности
            if word in sentiment_lexicon:
                current_label, current_score = sentiment_lexicon[word]
                
                # Применение отрицания
                if negation_active:
                    current_score *= -0.8
                    current_label = 'negative' if current_label == 'positive' else 'positive'
                    negation_active = False
                
                # Применение модификатора интенсивности
                if prev_word in intensifiers:
                    current_score *= intensifiers[prev_word]
                
                total_score += current_score
                matched_terms += 1
                word_matched = True
            
            prev_word = word if word_matched else None
        
        # Учет рейтинга (1-5 -> -1 до 1)
        rating_weight = (rating - 3) / 2.0
        
        # Комбинированная оценка
        if matched_terms > 0:
            final_score = (0.7 * (total_score / matched_terms)) + (0.3 * rating_weight)
        else:
            final_score = rating_weight

        final_score = round(final_score * 100) / 100    # Округление до двух знаков
        
        final_score = max(-1.0, min(1.0, final_score))
        
        # Определение метки
        if final_score > 0.2:
            label = 'positive'
        elif final_score < -0.2:
            label = 'negative'
        else:
            label = 'neutral'
        
        # Сохранение результатов
        cur.execute("""
            INSERT INTO sentiment_analysis (review_id, sentiment_score, sentiment_label)
            VALUES (%s, %s, %s)
            ON CONFLICT (review_id) DO UPDATE 
            SET sentiment_score = EXCLUDED.sentiment_score,
                sentiment_label = EXCLUDED.sentiment_label,
                created_at = CURRENT_TIMESTAMP
        """, (review_id, final_score, label))
    
    conn.commit()
    cur.close()


def main():
    """Основная функция выполнения"""
    conn = None
    try:
        with psycopg2.connect(**get_db_config()) as conn:

            print("Начало извлечения ключевых слов...")

            process_keywords_batch(conn, batch_size=1000)
            print("Извлечение ключевых слов завершено!")
            analyze_reviews_sentiment(conn)
            print("Анализ тональности завершен!")

        
    except Exception as e:
        print(f"Критическая ошибка: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
  