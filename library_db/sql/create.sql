CREATE TABLE IF NOT EXISTS role
(
    role_id   INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    role_code INT         NOT NULL UNIQUE,
    role_name VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS library_user
(
    user_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    passport_number VARCHAR(20)  NOT NULL UNIQUE,
    role_id         INT          NOT NULL,
    user_id_reg_by  BIGINT,
    last_name       VARCHAR(100) NOT NULL,
    first_name      VARCHAR(100) NOT NULL,
    middle_name     VARCHAR(100),
    address         VARCHAR(300),
    phone           VARCHAR(20)  NOT NULL,
    password_hash   VARCHAR(255) NOT NULL,

    CONSTRAINT fk_user_role
        FOREIGN KEY (role_id)
            REFERENCES role (role_id)
            ON DELETE RESTRICT
            ON UPDATE CASCADE,

    CONSTRAINT fk_user_id_reg_by
        FOREIGN KEY (user_id_reg_by)
            REFERENCES library_user (user_id)
            ON DELETE SET NULL
            ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS rubric
(
    rubric_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    parent_rubric_id   BIGINT,
    rubric_name        VARCHAR(200) NOT NULL,
    rubric_description VARCHAR(200),
    CONSTRAINT fk_rubric_parent
        FOREIGN KEY (parent_rubric_id)
            REFERENCES rubric (rubric_id)
            ON DELETE SET NULL
            ON UPDATE CASCADE,
    CONSTRAINT parent_current_rubrics_key
        UNIQUE (parent_rubric_id, rubric_name)
);

CREATE TABLE IF NOT EXISTS publisher
(
    publisher_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    publisher_name VARCHAR(200) NOT NULL,
    city           VARCHAR(100) NOT NULL,
    description    VARCHAR(500),
    CONSTRAINT uq_publisher_name_city
        UNIQUE (publisher_name, city)
);

CREATE TABLE IF NOT EXISTS author
(
    author_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    middle_name VARCHAR(100),
    bio         TEXT
);

CREATE TABLE IF NOT EXISTS ticket
(
    ticket_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ticket_number    VARCHAR(20) NOT NULL UNIQUE,
    owner_user_id    BIGINT      NOT NULL,
    operator_user_id BIGINT      NOT NULL,
    issue_date       DATE        NOT NULL,
    expire_date      DATE        NOT NULL,
    is_active        BOOLEAN     NOT NULL DEFAULT TRUE,
    CONSTRAINT fk_ticket_owner
        FOREIGN KEY (owner_user_id)
            REFERENCES library_user (user_id)
            ON DELETE RESTRICT
            ON UPDATE CASCADE,
    CONSTRAINT fk_ticket_operator_by
        FOREIGN KEY (operator_user_id)
            REFERENCES library_user (user_id)
            ON DELETE RESTRICT
            ON UPDATE CASCADE,
    CONSTRAINT chk_ticket_dates
        CHECK (expire_date >= issue_date)
);

CREATE TABLE IF NOT EXISTS edition
(
    edition_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rubric_id        BIGINT       NOT NULL,
    publisher_id     BIGINT       NOT NULL,
    user_id_added_by BIGINT       NOT NULL,
    title            VARCHAR(200) NOT NULL,
    publish_year     INT          NOT NULL,
    pages            INT          NOT NULL,
    annotation       TEXT,
    total_count      INT          NOT NULL, 
    current_count    INT          NOT NULL,
    CONSTRAINT fk_edition_rubric
        FOREIGN KEY (rubric_id)
            REFERENCES rubric (rubric_id),
    CONSTRAINT fk_edition_publisher
        FOREIGN KEY (publisher_id)
            REFERENCES publisher (publisher_id),
    CONSTRAINT fk_edition_added_by
        FOREIGN KEY (user_id_added_by)
            REFERENCES library_user (user_id),
    CONSTRAINT uq_edition_title_publisher_year
        UNIQUE (title, publisher_id, publish_year),
    CONSTRAINT chk_edition_year
        CHECK (publish_year <= EXTRACT(YEAR FROM CURRENT_DATE)),
    CONSTRAINT chk_edition_pages
        CHECK (pages > 0),
    CONSTRAINT chk_edition_total_count
        CHECK (total_count >= 0),
    CONSTRAINT chk_edition_current_count
        CHECK (current_count >= 0 AND current_count <= total_count)
);

CREATE TABLE IF NOT EXISTS edition_author
(
    edition_id   BIGINT  NOT NULL,
    author_id    BIGINT  NOT NULL,
    author_order INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (edition_id, author_id),
    CONSTRAINT fk_edition_author_edition
        FOREIGN KEY (edition_id)
            REFERENCES edition (edition_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
    CONSTRAINT fk_edition_author_author
        FOREIGN KEY (author_id)
            REFERENCES author (author_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS book_request
(
    request_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ticket_id    BIGINT      NOT NULL,
    request_date DATE        NOT NULL DEFAULT CURRENT_DATE,
    status       VARCHAR(20) NOT NULL DEFAULT 'NEW',
    CONSTRAINT fk_book_request_ticket
        FOREIGN KEY (ticket_id)
            REFERENCES ticket (ticket_id),
    CONSTRAINT chk_book_request_status
        CHECK (status IN ('NEW', 'PROCESSED', 'CLOSED', 'CANCELLED'))
);

CREATE TABLE IF NOT EXISTS request_item
(
    request_id BIGINT NOT NULL,
    edition_id BIGINT NOT NULL,
    qty        BIGINT NOT NULL,
    PRIMARY KEY (request_id, edition_id),
    CONSTRAINT fk_request_item_request
        FOREIGN KEY (request_id)
            REFERENCES book_request (request_id)
            ON DELETE CASCADE,
    CONSTRAINT fk_request_item_edition
        FOREIGN KEY (edition_id)
            REFERENCES edition (edition_id)
            ON DELETE RESTRICT,
    CONSTRAINT legal_qty
        CHECK (qty > 0)
);

CREATE TABLE IF NOT EXISTS issue_doc
(
    issue_doc_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_id       BIGINT,
    ticket_id        BIGINT NOT NULL,
    operator_user_id BIGINT NOT NULL,
    issue_date       DATE   NOT NULL DEFAULT CURRENT_DATE,
    CONSTRAINT fk_issue_doc_request
        FOREIGN KEY (request_id)
            REFERENCES book_request (request_id)
            ON DELETE SET NULL,
    CONSTRAINT fk_issue_doc_ticket
        FOREIGN KEY (ticket_id)
            REFERENCES ticket (ticket_id),
    CONSTRAINT fk_issue_doc_operator
        FOREIGN KEY (operator_user_id)
            REFERENCES library_user (user_id)
);

CREATE TABLE IF NOT EXISTS issue_item
(
    issue_doc_id    BIGINT  NOT NULL,
    edition_id      BIGINT  NOT NULL,
    due_date        DATE    NOT NULL,
    return_date     DATE,
    renew_count     INTEGER NOT NULL DEFAULT 0,
    last_renew_date DATE,
    PRIMARY KEY (issue_doc_id, edition_id),
    CONSTRAINT fk_issue_item_issue_doc
        FOREIGN KEY (issue_doc_id)
            REFERENCES issue_doc (issue_doc_id)
            ON DELETE CASCADE,
    CONSTRAINT fk_issue_item_edition
        FOREIGN KEY (edition_id)
            REFERENCES edition (edition_id),
    CONSTRAINT chk_issue_item_renew_count CHECK (renew_count BETWEEN 0 AND 5)
);

CREATE TABLE IF NOT EXISTS refusal_reason
(
    reason_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reason_code        VARCHAR(20)  NOT NULL UNIQUE,
    reason_name        VARCHAR(200) NOT NULL UNIQUE,
    reason_description TEXT
);

CREATE TABLE IF NOT EXISTS refusal_doc
(
    refusal_doc_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_id       BIGINT NOT NULL,
    operator_user_id BIGINT NOT NULL,
    refusal_date     DATE   NOT NULL DEFAULT CURRENT_DATE,
    CONSTRAINT fk_refusal_doc_request
        FOREIGN KEY (request_id)
            REFERENCES book_request (request_id)
            ON DELETE CASCADE,
    CONSTRAINT fk_refusal_doc_operator
        FOREIGN KEY (operator_user_id)
            REFERENCES library_user (user_id)
);

CREATE TABLE IF NOT EXISTS refusal_item
(
    refusal_doc_id BIGINT       NOT NULL,
    edition_id     BIGINT       NOT NULL,
    reason_id      BIGINT,
    reason_text    VARCHAR(500),
    PRIMARY KEY (refusal_doc_id, edition_id),
    CONSTRAINT fk_refusal_item_doc
        FOREIGN KEY (refusal_doc_id) REFERENCES
            refusal_doc (refusal_doc_id)
            ON DELETE CASCADE,
    CONSTRAINT fk_refusal_item_edition
        FOREIGN KEY (edition_id)
            REFERENCES edition (edition_id),
    CONSTRAINT fk_refusal_item_reason
        FOREIGN KEY (reason_id)
            REFERENCES refusal_reason (reason_id),
    CONSTRAINT chk_refusal_item_reason
        CHECK (reason_id IS NOT NULL OR reason_text IS NOT NULL)
);
