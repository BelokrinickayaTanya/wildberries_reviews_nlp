# wildberries_reviews_nlp
Учебный проект.

Проект wildberries_reviews_analysis представляет собой комплексное решение для анализа отзывов с маркетплейса Wildberries. Система собирает, обрабатывает и анализирует пользовательские отзывы с применением NLP-технологий.
Создание базы данных через wsl + DBeaver

Структура проекта
1. База данных (database/)
01_tables.sql - Создание структуры БД (товары, пользователи, отзывы, тональность, ключевые фразы, эмбеддинги)

02_functions.sql - Аналитические функции на PL/pgSQL

03_sample_data.sql - Тестовые данные для проверки работы

2. Обработка данных (python/)
db_connection.py - Подключение к PostgreSQL

data_loader.py - Импорт данных из CSV в БД

nlp_processor.py - Анализ тональности и извлечение ключевых фраз

embedding_generator.py - Генерация векторных представлений отзывов

3. Аналитика (analysis/)
basic_analysis.sql - Стандартные SQL-запросы для анализа

advanced_analysis.py - Продвинутая аналитика на Python

4. Конфигурация (config/)
.env.example - Шаблон для переменных окружения

requirements.txt - Зависимости Python

