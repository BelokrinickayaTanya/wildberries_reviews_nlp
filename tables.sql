-- Создание таблиц:
--Информация о товарах.

CREATE TABLE products (
	product_id SERIAL PRIMARY KEY, 
	product_name TEXT NOT NULL, 
	description TEXT, 
	has_sizes BOOLEAN, 
	color TEXT, 
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--Данные пользователей (авторов отзывов).
CREATE TABLE users (
user_id SERIAL PRIMARY KEY, 
user_name TEXT NOT NULL, 
gender TEXT, 
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--Отзывы с привязкой к товарам и пользователям.
CREATE TABLE reviews (
review_id SERIAL PRIMARY KEY, 
user_id INT NOT NULL references users(user_id),
product_id INT NOT NULL references products(product_id),
review_text TEXT NOT NULL, 
rating INT check (rating >= 1 AND rating <=5), 
matching_size TEXT NOT NULL, 
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--Результаты анализа тональности (оценка и метка).
CREATE TABLE sentsentiment_analysis (
review_id BIGINT unique, 
sentiment_score FLOAT NOT NULL check (sentiment_score BETWEEN -1 AND 1),
sentiment_label VARCHAR(20) NOT NULL CHECK (
	sentiment_label IN ('positive', 'neutral', 'negative')), 
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
FOREIGN KEY (review_id) REFERENCES reviews (review_id)
);
в таблицу
-- Извлеченные ключевые фразы.
CREATE TABLE keywords (
review_id BIGINT UNIQUE,
keywords TEXT[] NOT NULL,
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
PRIMARY KEY (review_id, keywords),
FOREIGN KEY (review_id) REFERENCES reviews (review_id)
);

-- Создаем таблицу для хранения эмбеддингов отзывов
CREATE TABLE review_embeddings (
    embedding_id SERIAL PRIMARY KEY,
    review_id INT NOT NULL REFERENCES reviews(review_id) ON DELETE CASCADE,
    embedding vector(384) NOT NULL,
    model_version VARCHAR(50) NOT NULL DEFAULT 'paraphrase-multilingual-MiniLM-L12-v2',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_review_embedding UNIQUE (review_id)   
);


Индексы для часто используемых полей.
CREATE INDEX idx_reviews_product_id ON reviews(product_id);
CREATE INDEX idx_reviews_user_id ON reviews(user_id);
CREATE INDEX idx_reviews_rating ON reviews(rating);
CREATE INDEX idx_embeddings_review_id ON embeddings(review_id);
