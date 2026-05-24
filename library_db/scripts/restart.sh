set -euo pipefail

echo "Пересоздание базы library_db"

PYTHON_BIN="python3"
if [ -x ".venv/bin/python3" ]; then
  PYTHON_BIN=".venv/bin/python3"
fi

echo "Удаление базы"
dropdb library_db --if-exists --force

echo "Создание базы"
createdb library_db

echo "Применение create.sql"
psql -d library_db -f ../sql/create.sql

echo "Применение seed.sql"
psql -d library_db -f ../sql/seed.sql

echo "Запуск генератора тестовых данных"
"$PYTHON_BIN" generate_test_data.py

echo "Применение триггеров"
psql -d library_db -f ../sql/triggers/create.sql

echo "Применение процедур"
psql -d library_db -f ../sql/procedures/create.sql

echo "Готово"
