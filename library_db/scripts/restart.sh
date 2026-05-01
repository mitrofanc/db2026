echo "Пересоздание базы library_db"

echo "Удаление базы"
dropdb library_db --if-exists --force

echo "Создание базы"
createdb library_db

echo "Применение create.sql"
psql -d library_db -f ../sql/create.sql

echo "Применение seed.sql"
psql -d library_db -f ../sql/seed.sql

echo "Запуск генератора тестовых данных"
python3 generate_test_data.py

echo "Применение триггеров"
psql -d library_db -f ../sql/triggers/create.sql

echo "Применение процедур"
psql -d library_db -f ../sql/procedures/create.sql

echo "Готово"
