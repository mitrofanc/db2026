import hashlib
import os
import psycopg2
import random
from datetime import date, timedelta

DB_NAME = os.getenv("DB_NAME", "library_db")
DB_USER = os.getenv("DB_USER", "dandreev")
DB_PASSWORD = os.getenv("DB_PASSWORD", os.getenv("PGPASSWORD", ""))
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

RANDOM_SEED = int(os.getenv("RANDOM_SEED", "42"))
READERS_COUNT = int(os.getenv("READERS_COUNT", "60"))
OPERATORS_COUNT = int(os.getenv("OPERATORS_COUNT", "4"))
BIBLIOGRAPHERS_COUNT = int(os.getenv("BIBLIOGRAPHERS_COUNT", "3"))
PUBLISHERS_COUNT = int(os.getenv("PUBLISHERS_COUNT", "18"))
AUTHORS_COUNT = int(os.getenv("AUTHORS_COUNT", "40"))
EDITIONS_COUNT = int(os.getenv("EDITIONS_COUNT", "90"))
REQUESTS_COUNT = int(os.getenv("REQUESTS_COUNT", "120"))

random.seed(RANDOM_SEED)
DATA_YEAR = 2024
YEAR_START = date(DATA_YEAR, 1, 1)
TODAY = date(DATA_YEAR, 12, 31)

LAST_NAMES = [
    "Иванов", "Петров", "Сидоров", "Смирнов", "Кузнецов", "Попов", "Соколов", "Лебедев",
    "Козлов", "Новиков", "Морозов", "Волков", "Алексеев", "Федоров", "Белов", "Степанов",
    "Павлов", "Семенов", "Захаров", "Николаев",
]

FIRST_NAMES = [
    "Алексей", "Иван", "Дмитрий", "Мария", "Анна", "Ольга", "Сергей", "Павел",
    "Максим", "Наталья", "Елена", "Ирина", "Кирилл", "Юлия", "Андрей", "Татьяна",
]

MIDDLE_NAMES = [
    "Алексеевич", "Иванович", "Дмитриевич", "Сергеевич", "Павлович",
    "Алексеевна", "Ивановна", "Дмитриевна", "Сергеевна", "Павловна",
]

CITIES = ["Москва", "Санкт-Петербург", "Казань", "Томск", "Новосибирск", "Самара", "Пермь"]

STREETS = ["Ленина", "Мира", "Пушкина", "Гагарина", "Школьная", "Садовая", "Новая"]

PUBLISHER_WORDS = ["Наука", "Мир", "Прогресс", "Питер", "Бином", "Просвещение", "Университет"]

TITLE_PREFIXES = ["Основы", "Введение в", "Практикум по", "Курс", "Справочник по", "Теория"]

TITLE_TOPICS = [
    "базам данных", "программированию", "математике", "физике", "истории России",
    "русской литературе", "фантастике", "алгоритмам", "SQL", "системному анализу",
]


def random_person():
    return (
        random.choice(LAST_NAMES),
        random.choice(FIRST_NAMES),
        random.choice(MIDDLE_NAMES),
    )


def random_address():
    return f"г. {random.choice(CITIES)}, ул. {random.choice(STREETS)}, д. {random.randint(1, 150)}"


def random_phone(idx):
    return f"+79{random.randint(10, 99)}{random.randint(1000000, 9999999)}{idx % 10}"


def password_hash(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def numeric_passport(series_code, idx):
    return f"{series_code:04d}{idx + 1:06d}"


def random_date_between(start_date=YEAR_START, end_date=TODAY):
    return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))


def fetch_ids(cur, query, params=None):
    cur.execute(query, params or ())
    return [row[0] for row in cur.fetchall()]

def fetch_one(cur, query, params=None):
    cur.execute(query, params or ())
    row = cur.fetchone()
    return row[0] if row else None


def get_role_id(cur, role_code):
    return fetch_one(cur, "SELECT role_id FROM role WHERE role_code = %s", (role_code,))


def get_reason_id(cur, reason_code):
    return fetch_one(cur, "SELECT reason_id FROM refusal_reason WHERE reason_code = %s", (reason_code,))

# сколько книг на руках у читателя
def active_loans_count(cur, ticket_id):
    cur.execute(
        """
        SELECT COUNT(*)
        FROM issue_doc d
        JOIN issue_item i ON i.issue_doc_id = d.issue_doc_id
        WHERE d.ticket_id = %s
          AND i.return_date IS NULL
        """,
        (ticket_id,),
    )
    return cur.fetchone()[0]


def has_overdue(cur, ticket_id):
    cur.execute(
        """
        SELECT EXISTS(
            SELECT 1
            FROM issue_doc d
            JOIN issue_item i ON i.issue_doc_id = d.issue_doc_id
            WHERE d.ticket_id = %s
              AND i.return_date IS NULL
              AND i.due_date < %s
        )
        """,
        (ticket_id, TODAY),
    )
    return cur.fetchone()[0]

#  количество доступных книг
def available_count(cur, edition_id):
    return fetch_one(cur, "SELECT current_count FROM edition WHERE edition_id = %s", (edition_id,))


def decrement_current_count(cur, edition_id, amount=1):
    cur.execute(
        "UPDATE edition SET current_count = current_count - %s WHERE edition_id = %s",
        (amount, edition_id),
    )


def increment_current_count(cur, edition_id, amount=1):
    cur.execute(
        "UPDATE edition SET current_count = current_count + %s WHERE edition_id = %s",
        (amount, edition_id),
    )


conn_params = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "host": DB_HOST,
    "port": DB_PORT,
}
if DB_PASSWORD:
    conn_params["password"] = DB_PASSWORD

conn = psycopg2.connect(**conn_params)
cur = conn.cursor()
cur.execute("SELECT current_database(), current_user")
_db_name, _db_user = cur.fetchone()
print(f"Connected to: {_db_name}, user: {_db_user}")

# взяли менеджера
manager_id = fetch_one(
    cur,
    """
    SELECT u.user_id
    FROM library_user u
    JOIN role r ON r.role_id = u.role_id
    WHERE r.role_code = 3
    ORDER BY u.user_id
    LIMIT 1
    """,
)

# взяли операторов
operator_ids = fetch_ids(
    cur,
    """
    SELECT u.user_id
    FROM library_user u
    JOIN role r ON r.role_id = u.role_id
    WHERE r.role_code = 2
    ORDER BY u.user_id
    """,
)

# взяли библиографов
bibliographer_ids = fetch_ids(
    cur,
    """
    SELECT u.user_id
    FROM library_user u
    JOIN role r ON r.role_id = u.role_id
    WHERE r.role_code = 1
    ORDER BY u.user_id
    """,
)

# остальные сотрудники
operator_role_id = get_role_id(cur, 2)
biblio_role_id = get_role_id(cur, 1)
reader_role_id = get_role_id(cur, 4)

for i in range(OPERATORS_COUNT):
    ln, fn, mn = random_person()
    passport = numeric_passport(1100, i)
    cur.execute(
        """
        INSERT INTO library_user (
            passport_number, role_id, user_id_reg_by,
            last_name, first_name, middle_name,
            address, phone, password_hash
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING user_id
        """,
        (
            passport,
            operator_role_id,
            manager_id,
            ln, fn, mn,
            random_address(),
            random_phone(i),
            password_hash(passport),
        ),
    )
    operator_ids.append(cur.fetchone()[0])

for i in range(BIBLIOGRAPHERS_COUNT):
    ln, fn, mn = random_person()
    passport = numeric_passport(1200, i)
    cur.execute(
        """
        INSERT INTO library_user (
            passport_number, role_id, user_id_reg_by,
            last_name, first_name, middle_name,
            address, phone, password_hash
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING user_id
        """,
        (
            passport,
            biblio_role_id,
            manager_id,
            ln, fn, mn,
            random_address(),
            random_phone(i + 1000),
            password_hash(passport),
        ),
    )
    bibliographer_ids.append(cur.fetchone()[0])

# читатели и билеты
reader_ids = []
ticket_ids = []

for i in range(READERS_COUNT):
    ln, fn, mn = random_person()
    passport = numeric_passport(1300, i)
    reg_by = random.choice(operator_ids)
    cur.execute(
        """
        INSERT INTO library_user (
            passport_number, role_id, user_id_reg_by,
            last_name, first_name, middle_name,
            address, phone, password_hash
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING user_id
        """,
        (
            passport,
            reader_role_id,
            reg_by,
            ln, fn, mn,
            random_address(),
            random_phone(i + 2000),
            password_hash(passport),
        ),
    )
    reader_id = cur.fetchone()[0]
    reader_ids.append(reader_id)

    issue_date = random_date_between()
    expire_date = issue_date + timedelta(days=365 * 5)

    # часть билетов делаем просроченными
    if random.random() < 0.12 and issue_date < TODAY:
        expire_date = random_date_between(issue_date, TODAY - timedelta(days=1))
        is_active = False
    else:
        is_active = True

    cur.execute(
        """
        INSERT INTO ticket (
            ticket_number, owner_user_id, operator_user_id,
            issue_date, expire_date, is_active
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        RETURNING ticket_id
        """,
        (
            f"TKT{i:07d}",
            reader_id,
            random.choice(operator_ids),
            issue_date,
            expire_date,
            is_active,
        ),
    )
    ticket_ids.append(cur.fetchone()[0])

    # часть читателей получает архивный билет, чтобы запрос 04 находил пользователей с перевыпуском
    if i < max(1, READERS_COUNT // 5):
        archived_issue_date = random_date_between(YEAR_START, TODAY - timedelta(days=1))
        archived_expire_date = random_date_between(archived_issue_date, TODAY - timedelta(days=1))
        cur.execute(
            """
            INSERT INTO ticket (
                ticket_number, owner_user_id, operator_user_id,
                issue_date, expire_date, is_active
            )
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (
                f"TKT{READERS_COUNT + i:07d}",
                reader_id,
                random.choice(operator_ids),
                archived_issue_date,
                archived_expire_date,
                False,
            ),
        )

# издательства
publisher_ids = []
for i in range(PUBLISHERS_COUNT):
    publisher_name = f"{random.choice(PUBLISHER_WORDS)} {i + 1}"
    city = random.choice(CITIES)
    cur.execute(
        """
        INSERT INTO publisher (publisher_name, city, description)
        VALUES (%s,%s,%s)
        RETURNING publisher_id
        """,
        (
            publisher_name,
            city,
            f"Издательство {publisher_name}, город {city}",
        ),
    )
    publisher_ids.append(cur.fetchone()[0])

# Авторы
author_ids = []
for i in range(AUTHORS_COUNT):
    ln, fn, mn = random_person()
    cur.execute(
        """
        INSERT INTO author (first_name, last_name, middle_name, bio)
        VALUES (%s,%s,%s,%s)
        RETURNING author_id
        """,
        (
            fn,
            ln,
            mn,
            f"{fn} {mn} {ln} — тестовый автор №{i + 1}",
        ),
    )
    author_ids.append(cur.fetchone()[0])

# Издания и связь издание-автор
rubric_ids = fetch_ids(cur, "SELECT rubric_id FROM rubric ORDER BY rubric_id")
edition_ids = []

for i in range(EDITIONS_COUNT):
    title = f"{random.choice(TITLE_PREFIXES)} {random.choice(TITLE_TOPICS)} #{i + 1}"
    publish_year = random.randint(max(1950, TODAY.year - 20), TODAY.year)
    total_count = random.randint(2, 12)

    cur.execute(
        """
        INSERT INTO edition (
            rubric_id, publisher_id, user_id_added_by,
            title, publish_year, pages, annotation,
            total_count, current_count
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING edition_id
        """,
        (
            random.choice(rubric_ids),
            random.choice(publisher_ids),
            random.choice(bibliographer_ids),
            title,
            publish_year,
            random.randint(120, 950),
            f"Тестовая аннотация к изданию '{title}'",
            total_count,
            total_count,
        ),
    )
    edition_id = cur.fetchone()[0]
    edition_ids.append(edition_id)

    linked_authors = random.sample(author_ids, random.randint(1, min(3, len(author_ids))))
    order_num = 1
    for author_id in linked_authors:
        cur.execute(
            """
            INSERT INTO edition_author (edition_id, author_id, author_order)
            VALUES (%s,%s,%s)
            """,
            (edition_id, author_id, order_num),
        )
        order_num += 1

# начальные выдачи: чтобы были долги, активные книги и история возвратов
for _ in range(max(10, READERS_COUNT // 3)):
    ticket_id = random.choice(ticket_ids)
    operator_id = random.choice(operator_ids)
    issue_date = random_date_between()

    cur.execute(
        """
        INSERT INTO issue_doc (request_id, ticket_id, operator_user_id, issue_date)
        VALUES (%s,%s,%s,%s)
        RETURNING issue_doc_id
        """,
        (None, ticket_id, operator_id, issue_date),
    )
    issue_doc_id = cur.fetchone()[0]

    count_items = random.randint(1, 3)
    selected_editions = random.sample(edition_ids, count_items)

    for edition_id in selected_editions:
        due_date = issue_date + timedelta(days=30)
        scenario = random.random()

        # возвращена вовремя/с опозданием
        if scenario < 0.35:
            return_date = due_date - timedelta(days=random.randint(0, 7))
            if return_date < issue_date:
                return_date = issue_date + timedelta(days=3)
            cur.execute(
                """
                INSERT INTO issue_item (issue_doc_id, edition_id, due_date, return_date, renew_count, last_renew_date)
                VALUES (%s,%s,%s,%s,%s,%s)
                """,
                (issue_doc_id, edition_id, due_date, return_date, 0, None),
            )
        # просрочена и не возвращена
        elif scenario < 0.55:
            overdue_issue_date = random_date_between(YEAR_START, TODAY - timedelta(days=31))
            overdue_due_date = overdue_issue_date + timedelta(days=30)
            cur.execute("UPDATE issue_doc SET issue_date = %s WHERE issue_doc_id = %s",
                        (overdue_issue_date, issue_doc_id))
            cur.execute(
                """
                INSERT INTO issue_item (issue_doc_id, edition_id, due_date, return_date, renew_count, last_renew_date)
                VALUES (%s,%s,%s,%s,%s,%s)
                """,
                (issue_doc_id, edition_id, overdue_due_date, None, 0, None),
            )
            decrement_current_count(cur, edition_id)
        # активна, не просрочена
        elif scenario < 0.80:
            active_issue_date = random_date_between(TODAY - timedelta(days=29), TODAY)
            active_due_date = active_issue_date + timedelta(days=30)
            cur.execute("UPDATE issue_doc SET issue_date = %s WHERE issue_doc_id = %s",
                        (active_issue_date, issue_doc_id))
            cur.execute(
                """
                INSERT INTO issue_item (issue_doc_id, edition_id, due_date, return_date, renew_count, last_renew_date)
                VALUES (%s,%s,%s,%s,%s,%s)
                """,
                (issue_doc_id, edition_id, active_due_date, None, 0, None),
            )
            decrement_current_count(cur, edition_id)
        # продлённая выдача
        else:
            renew_count = random.randint(1, 3)
            renew_issue_date = random_date_between(
                YEAR_START,
                TODAY - timedelta(days=30 * (renew_count + 1)),
            )
            due_date = renew_issue_date + timedelta(days=30 * (renew_count + 1))
            last_renew_date = due_date - timedelta(days=30)
            cur.execute("UPDATE issue_doc SET issue_date = %s WHERE issue_doc_id = %s",
                        (renew_issue_date, issue_doc_id))
            cur.execute(
                """
                INSERT INTO issue_item (issue_doc_id, edition_id, due_date, return_date, renew_count, last_renew_date)
                VALUES (%s,%s,%s,%s,%s,%s)
                """,
                (issue_doc_id, edition_id, due_date, None, renew_count, last_renew_date),
            )
            decrement_current_count(cur, edition_id)

# запросы, выдачи и отказы
# Ограничения:
# - не более 10 книг на руках
# - срок выдачи 1 месяц
# - продлений не более 5
# - отказ при просрочках / истекшем билете / отсутствии экземпляра / превышении лимита

reason_no_stock = get_reason_id(cur, "NO_COPIES")
reason_ticket_expired = get_reason_id(cur, "TICKET_EXPIRED")
reason_limit = get_reason_id(cur, "RULES_VIOLATION")
reason_overdue = get_reason_id(cur, "RULES_VIOLATION")

for i in range(REQUESTS_COUNT):
    ticket_id = random.choice(ticket_ids)
    operator_id = random.choice(operator_ids)
    request_date = random_date_between()

    cur.execute(
        """
        INSERT INTO book_request (ticket_id, request_date, status)
        VALUES (%s,%s,%s)
        RETURNING request_id
        """,
        (ticket_id, request_date, "NEW"),
    )
    request_id = cur.fetchone()[0]

    editions_in_request = random.sample(edition_ids, random.randint(1, 4))
    for edition_id in editions_in_request:
        cur.execute(
            "INSERT INTO request_item (request_id, edition_id, qty) VALUES (%s,%s,%s)",
            (request_id, edition_id, 1),
        )

    ticket_info = fetch_one(
        cur,
        "SELECT is_active FROM ticket WHERE ticket_id = %s",
        (ticket_id,),
    )
    expire_date = fetch_one(
        cur,
        "SELECT expire_date FROM ticket WHERE ticket_id = %s",
        (ticket_id,),
    )

    issued_editions = []
    refused_editions = []
    current_loans = active_loans_count(cur, ticket_id)
    overdue_exists = has_overdue(cur, ticket_id)

    for edition_id in editions_in_request:
        if (not ticket_info) or expire_date < TODAY:
            refused_editions.append((edition_id, reason_ticket_expired, None))
            continue

        if overdue_exists:
            refused_editions.append((edition_id, reason_overdue, None))
            continue

        if current_loans >= 10:
            refused_editions.append((edition_id, reason_limit, None))
            continue

        if available_count(cur, edition_id) <= 0:
            refused_editions.append((edition_id, reason_no_stock, None))
            continue

        issued_editions.append(edition_id)
        current_loans += 1

    if issued_editions:
        cur.execute(
            """
            INSERT INTO issue_doc (request_id, ticket_id, operator_user_id, issue_date)
            VALUES (%s,%s,%s,%s)
            RETURNING issue_doc_id
            """,
            (request_id, ticket_id, operator_id, request_date),
        )
        issue_doc_id = cur.fetchone()[0]

        for edition_id in issued_editions:
            due_date = request_date + timedelta(days=30)
            renew_count = 0
            last_renew_date = None
            return_date = None

            # часть новых выдач делаем уже возвращёнными / продлёнными для разнообразия
            scenario = random.random()
            if scenario < 0.25:
                return_date = request_date + timedelta(days=random.randint(3, 25))
                if return_date > TODAY:
                    return_date = TODAY
            elif scenario < 0.40:
                renew_count = random.randint(1, 2)
                due_date = due_date + timedelta(days=30 * renew_count)
                last_renew_date = due_date - timedelta(days=30)

            cur.execute(
                """
                INSERT INTO issue_item (issue_doc_id, edition_id, due_date, return_date, renew_count, last_renew_date)
                VALUES (%s,%s,%s,%s,%s,%s)
                """,
                (issue_doc_id, edition_id, due_date, return_date, renew_count, last_renew_date),
            )

            if return_date is None:
                decrement_current_count(cur, edition_id)

        new_status = "PROCESSED"
    else:
        new_status = "CANCELLED"

    if refused_editions:
        cur.execute(
            """
            INSERT INTO refusal_doc (request_id, operator_user_id, refusal_date)
            VALUES (%s,%s,%s)
            RETURNING refusal_doc_id
            """,
            (request_id, operator_id, request_date),
        )
        refusal_doc_id = cur.fetchone()[0]

        for edition_id, reason_id, reason_text in refused_editions:
            if reason_id is None and reason_text is None:
                reason_text = "Нарушены правила библиотеки"
            cur.execute(
                """
                INSERT INTO refusal_item (refusal_doc_id, edition_id, reason_id, reason_text)
                VALUES (%s,%s,%s,%s)
                """,
                (refusal_doc_id, edition_id, reason_id, reason_text),
            )

        if issued_editions:
            new_status = "PROCESSED"
        else:
            new_status = "CLOSED"

    cur.execute(
        "UPDATE book_request SET status = %s WHERE request_id = %s",
        (new_status, request_id),
    )

# подсчет книг
cur.execute(
    """
    UPDATE edition e
    SET current_count = e.total_count - COALESCE(src.active_count, 0)
    FROM (
        SELECT i.edition_id, COUNT(*) AS active_count
        FROM issue_item i
        WHERE i.return_date IS NULL
        GROUP BY i.edition_id
    ) src
    WHERE e.edition_id = src.edition_id
    """
)

cur.execute(
    """
    UPDATE edition e
    SET current_count = e.total_count
    WHERE NOT EXISTS (
        SELECT 1 FROM issue_item i
        WHERE i.edition_id = e.edition_id
          AND i.return_date IS NULL
    )
    """
)

conn.commit()
cur.close()
conn.close()

print("Тестовые данные для библиотеки успешно сгенерированы.")
print(f"Читателей: {READERS_COUNT}")
print(f"Доп. операторов: {OPERATORS_COUNT}")
print(f"Доп. библиографов: {BIBLIOGRAPHERS_COUNT}")
print(f"Издательств: {PUBLISHERS_COUNT}")
print(f"Авторов: {AUTHORS_COUNT}")
print(f"Изданий: {EDITIONS_COUNT}")
print(f"Запросов: {REQUESTS_COUNT}")
