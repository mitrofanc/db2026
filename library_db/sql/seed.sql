BEGIN;

-- Роли
INSERT INTO role (role_code, role_name)
VALUES (1, 'BIBLIOGRAPHER'),
       (2, 'OPERATOR'),
       (3, 'MANAGER'),
       (4, 'READER');

-- Стандартные причины отказа
INSERT INTO refusal_reason (reason_code, reason_name, reason_description)
VALUES ('NO_COPIES', 'Нет свободного экземпляра', 'На данный момент все экземпляры издания выданы'),
       ('RULES_VIOLATION', 'Нарушены правила библиотеки',
        'Превышен лимит книг, есть просрочка или иное нарушение правил'),
       ('TICKET_EXPIRED', 'Истек срок действия читательского билета',
        'Операции по билету невозможны до продления или перевыпуска'),
       ('RENEW_LIMIT', 'Превышен лимит продлений',
        'Для данного издания достигнуто максимально допустимое число продлений');

-- Корневые рубрики
INSERT INTO rubric (parent_rubric_id, rubric_name, rubric_description)
VALUES (NULL, 'Художественная литература', 'Романы, рассказы, повести, поэзия'),
       (NULL, 'Научная литература', 'Научные и учебные издания'),
       (NULL, 'Техническая литература', 'Инженерия, ИТ, прикладные дисциплины'),
       (NULL, 'История', 'Исторические исследования и документы');

-- Дочерние рубрики
INSERT INTO rubric (parent_rubric_id, rubric_name, rubric_description)
SELECT r.rubric_id, 'Русская литература', 'Произведения русских авторов'
FROM rubric r
WHERE r.rubric_name = 'Художественная литература';

INSERT INTO rubric (parent_rubric_id, rubric_name, rubric_description)
SELECT r.rubric_id, 'Зарубежная литература', 'Произведения зарубежных авторов'
FROM rubric r
WHERE r.rubric_name = 'Художественная литература';

INSERT INTO rubric (parent_rubric_id, rubric_name, rubric_description)
SELECT r.rubric_id, 'Математика', 'Учебная и научная литература по математике'
FROM rubric r
WHERE r.rubric_name = 'Научная литература';

INSERT INTO rubric (parent_rubric_id, rubric_name, rubric_description)
SELECT r.rubric_id, 'Физика', 'Учебная и научная литература по физике'
FROM rubric r
WHERE r.rubric_name = 'Научная литература';

INSERT INTO rubric (parent_rubric_id, rubric_name, rubric_description)
SELECT r.rubric_id, 'Программирование', 'Книги по разработке ПО и алгоритмам'
FROM rubric r
WHERE r.rubric_name = 'Техническая литература';

INSERT INTO rubric (parent_rubric_id, rubric_name, rubric_description)
SELECT r.rubric_id, 'Базы данных', 'Книги по проектированию и эксплуатации БД'
FROM rubric r
WHERE r.rubric_name = 'Техническая литература';

-- Базовые сотрудники

-- 1. Заведующий
INSERT INTO library_user (passport_number,
                          role_id,
                          user_id_reg_by,
                          last_name,
                          first_name,
                          middle_name,
                          address,
                          phone,
                          password_hash)
VALUES ('98427520',
        (SELECT role_id FROM role WHERE role_code = 3),
        NULL,
        'Смирнов',
        'Алексей',
        'Игоревич',
        'г. Москва, ул. Центральная, д. 1',
        '+79990000001',
        'manager_hash');

-- 2. Оператор
INSERT INTO library_user (passport_number,
                          role_id,
                          user_id_reg_by,
                          last_name,
                          first_name,
                          middle_name,
                          address,
                          phone,
                          password_hash)
VALUES ('98435893',
        (SELECT role_id FROM role WHERE role_code = 2),
        (SELECT user_id FROM library_user WHERE passport_number = 'MANAGER-000001'),
        'Петрова',
        'Марина',
        'Сергеевна',
        'г. Москва, ул. Библиотечная, д. 5',
        '+79990000002',
        'operator_hash');

-- 3. Библиограф
INSERT INTO library_user (passport_number,
                          role_id,
                          user_id_reg_by,
                          last_name,
                          first_name,
                          middle_name,
                          address,
                          phone,
                          password_hash)
VALUES ('11203958',
        (SELECT role_id FROM role WHERE role_code = 1),
        (SELECT user_id FROM library_user WHERE passport_number = 'MANAGER-000001'),
        'Иванов',
        'Дмитрий',
        'Олегович',
        'г. Москва, ул. Книжная, д. 7',
        '+79990000003',
        'biblio_hash');
COMMIT;