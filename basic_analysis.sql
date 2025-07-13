
---Стандартные проверки + базовые аналитические функции
-- Вывод всех записей из таблицы товаров (products)
select * from products;


SELECT user_id FROM users;

-- Вывод всех записей из таблицы товаров (users)
select * from users;

-- Вывод всех записей из таблицы товаров (keywords)
select * from keywords;

-- Вывод всех записей из таблицы товаров (reviews)
select * from reviews;

-- Анализ максимальной длины отзывов (в словах) в таблице reviews
SELECT MAX(array_length(regexp_split_to_array(review_text, '\s+'), 1)) AS max_word_count
FROM reviews;

-- Нахождение минимального количества слов в отзывах из таблицы reviews
SELECT MIN(array_length(regexp_split_to_array(trim(review_text), '\s+'), 1)) AS min_word_count
FROM reviews;

-- Вычисление средней длины отзывов в словах (с округлением до 2 знаков после запятой)
SELECT ROUND(AVG(array_length(regexp_split_to_array(trim(review_text), '\s+'), 1)), 2) AS avg_word_count
FROM reviews;

-- Поиск самого длинного отзыва по количеству слов
SELECT 
  review_id,
  review_text,
  array_length(regexp_split_to_array(trim(review_text), '\s+'), 1) AS word_count
FROM reviews
ORDER BY word_count DESC
LIMIT 1;


-- Вывод всех записей из таблицы товаров (review_embeddings)
select * from review_embeddings; 

-- Вывод всех записей из таблицы товаров (sentiment_analysis)
select * from sentiment_analysis; 


SELECT COUNT(review_id) AS total_review_ids
FROM keywords;

SELECT * FROM find_similar_reviews_by_embedding(122352);

select * from analyze_sentiment_by_category();

DROP FUNCTION IF EXISTS analyze_sentiment_by_category(integer, float, integer);

SELECT COUNT(product_id) AS total_product_ids
FROM products;

SELECT COUNT(review_id) AS total_preview_ids
FROM keywords;

SELECT COUNT(review_id) AS total_preview_ids
FROM sentiment_analysis;

SELECT COUNT(review_text) AS total_preview_text
FROM reviews;

-- Для очистки таблиц:
-- Начните транзакцию
BEGIN;

-- Очистка таблиц с сохранением структуры
TRUNCATE TABLE products RESTART IDENTITY CASCADE;

TRUNCATE TABLE keywords RESTART IDENTITY CASCADE;

TRUNCATE TABLE sentiment_analysis RESTART IDENTITY CASCADE;

TRUNCATE TABLE review_embeddings RESTART IDENTITY CASCADE;

TRUNCATE TABLE users RESTART IDENTITY CASCADE;


-- Завершите транзакцию
COMMIT;

-- Распределение оценок по всем отзывам
SELECT
    rating,
    COUNT(*) as review_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
FROM
    reviews
GROUP BY
    rating
ORDER BY
    rating;


-- Зависимость между длиной отзыва и оценкой
SELECT
    rating,
    AVG(LENGTH (review_text)) as avg_length,
    COUNT(*) as review_count
FROM
    reviews
GROUP BY
    rating
ORDER BY
    rating;




-- Средний рейтинг с анализом тональности
SELECT 
    p.product_id,
    p.product_name,
    ROUND(AVG(r.rating)::numeric, 2) as avg_rating,
    ROUND(AVG(sa.sentiment_score)::numeric, 3) as avg_sentiment,
    CASE 
        WHEN AVG(sa.sentiment_score) > 0.2 THEN 'positive'
        WHEN AVG(sa.sentiment_score) < -0.2 THEN 'negative'
        ELSE 'neutral'
    END as sentiment_category,
    COUNT(*) as review_count
FROM 
    products p
    JOIN reviews r ON p.product_id = r.product_id
    JOIN sentiment_analysis sa ON r.review_id = sa.review_id
GROUP BY 
    p.product_id, p.product_name
HAVING 
    COUNT(*) >= 5
ORDER BY 
    avg_sentiment DESC;





--Анализ соотвествия размерам
SELECT 
    matching_size,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
FROM 
    reviews
GROUP BY 
    matching_size
ORDER BY 
    count DESC;


-- Отзывы с расхождением между оценкой и тональностью (аномальные)
SELECT 
    r.review_id,
    p.product_name,
    r.rating,
    sa.sentiment_label,
    sa.sentiment_score,
    r.matching_size,
    k.keywords,
    SUBSTRING(r.review_text, 1, 100) || '...' as review_excerpt
FROM 
    reviews r
    JOIN sentiment_analysis sa ON r.review_id = sa.review_id
    JOIN products p ON r.product_id = p.product_id
    LEFT JOIN keywords k ON r.review_id = k.review_id
WHERE 
    (r.rating <= 2 AND sa.sentiment_label = 'positive') OR
    (r.rating >= 4 AND sa.sentiment_label = 'negative')
ORDER BY 
    ABS(r.rating - CASE sa.sentiment_label 
                   WHEN 'positive' THEN 5 
                   WHEN 'negative' THEN 1 
                   ELSE 3 END) DESC,
    sa.sentiment_score DESC
LIMIT 20;

-- Средний рейтинг по продуктам
SELECT 
    p.product_id,
    p.product_name,
    ROUND(AVG(r.rating), 2) as avg_rating,
    COUNT(r.review_id) as review_count
FROM 
    products p
inner JOIN 
    reviews r ON p.product_id = r.product_id
GROUP BY 
    p.product_id, p.product_name
ORDER BY 
    avg_rating DESC;

--Соотношение позитивных/нейтральных/негативных отзывов:
SELECT 
    sentiment_label,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
FROM 
    sentiment_analysis
GROUP BY 
    sentiment_label
ORDER BY 
    count DESC;


-- Полнотекстовый поиск: с рейтингом
SELECT 
    r.review_id,
    r.product_id,
    p.product_name,
    r.rating,
    ts_rank_cd(to_tsvector('russian', r.review_text), query) as rank_score,
    ts_headline('russian', r.review_text, query) as highlight
FROM 
    reviews r
    JOIN products p ON r.product_id = p.product_id,
    plainto_tsquery('russian', 'нормально | пойдет') query
WHERE 
    to_tsvector('russian', r.review_text) @@ query
ORDER BY 
    r.rating DESC, 
    rank_score DESC
LIMIT 10;



