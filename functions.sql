--Аналитические функции
--Поиск похожих отзывов (по тексту и эмбеддингам).
create or replace function find_similar_reviews_by_embedding(
    target_review_id INT,
    similarity_threshold FLOAT DEFAULT 0.7,
    limit_count INT DEFAULT 5
)
RETURNS TABLE (
    similar_review_id INT,  
    product_id INT,
    similarity FLOAT,
    review_text TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH target_embedding AS (
        SELECT embedding 
        FROM review_embeddings 
        WHERE review_id = target_review_id
    )
    SELECT 
        r.review_id AS similar_review_id,  
        r.product_id,
        1 - (re.embedding <#> (SELECT embedding FROM target_embedding)) / 2 AS similarity,
        r.review_text
    FROM 
        review_embeddings re
        JOIN reviews r ON re.review_id = r.review_id
    WHERE 
        re.review_id != target_review_id AND
        1 - (re.embedding <#> (SELECT embedding FROM target_embedding)) / 2 > similarity_threshold
    ORDER BY 
        similarity DESC
    LIMIT 
        limit_count;
END;
$$ LANGUAGE plpgsql;


SELECT * FROM find_similar_reviews_by_embedding(123);



Анализ тональности по продуктам
CREATE MATERIALIZED VIEW mv_product_sentiment AS
SELECT
    p.product_id,
    p.product_name,
    ROUND(AVG(s.sentiment_score)::numeric, 3) AS avg_sentiment,
    COUNT(CASE WHEN s.sentiment_label = 'positive' THEN 1 END) AS positive_count,
    COUNT(CASE WHEN s.sentiment_label = 'negative' THEN 1 END) AS negative_count,
    COUNT(s.review_id) AS analyzed_reviews_count
FROM
    products p
    LEFT JOIN reviews r ON p.product_id = r.product_id
    LEFT JOIN sentiment_analysis s ON r.review_id = s.review_id
GROUP BY
    p.product_id, p.product_name
WITH DATA;

CREATE INDEX idx_mv_product_sentiment_id ON mv_product_sentiment(product_id);



SELECT * FROM mv_product_sentiment;
SELECT * FROM mv_product_sentiment WHERE avg_sentiment > 0.5;