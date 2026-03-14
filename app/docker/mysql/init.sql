
CREATE TABLE users (
    user_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
    email         VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    date_of_birth DATE NOT NULL,
    country_code  CHAR(2) NOT NULL,
    phone         VARCHAR(20),
    status        VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE profiles (
    profile_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id         BIGINT NOT NULL,
    name            VARCHAR(50) NOT NULL,
    avatar_url      VARCHAR(500),
    is_kids         BOOLEAN NOT NULL DEFAULT FALSE,
    maturity_rating VARCHAR(10) NOT NULL DEFAULT 'ALL',
    language        VARCHAR(5) NOT NULL DEFAULT 'pl',
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE subscriptions (
    subscription_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id         BIGINT NOT NULL,
    plan_name       VARCHAR(50) NOT NULL,
    price_monthly   DECIMAL(10,2) NOT NULL,
    max_streams     SMALLINT NOT NULL,
    max_resolution  VARCHAR(10) NOT NULL,
    max_profiles    SMALLINT NOT NULL DEFAULT 5,
    status          VARCHAR(20) NOT NULL DEFAULT 'active',
    start_date      DATE NOT NULL,
    end_date        DATE,
    auto_renew      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE payments (
    payment_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
    subscription_id BIGINT NOT NULL,
    amount          DECIMAL(10,2) NOT NULL,
    currency        CHAR(3) NOT NULL DEFAULT 'PLN',
    payment_method  VARCHAR(30) NOT NULL,
    transaction_id  VARCHAR(255) UNIQUE,
    status          VARCHAR(20) NOT NULL,
    paid_at         TIMESTAMP NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (subscription_id) REFERENCES subscriptions(subscription_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE content (
    content_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
    title             VARCHAR(300) NOT NULL,
    description       TEXT,
    type              VARCHAR(10) NOT NULL,
    release_date      DATE,
    duration_minutes  INT,
    maturity_rating   VARCHAR(10) NOT NULL,
    genres            VARCHAR(500),
    poster_url        VARCHAR(500),
    trailer_url       VARCHAR(500),
    avg_rating        DECIMAL(3,2) DEFAULT 0.00,
    total_views       BIGINT DEFAULT 0,
    popularity_score  DECIMAL(10,2) DEFAULT 0.00,
    country_of_origin CHAR(2),
    original_language VARCHAR(5),
    is_active         BOOLEAN NOT NULL DEFAULT TRUE,
    metadata          JSON,
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE seasons (
    season_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    content_id    BIGINT NOT NULL,
    season_number SMALLINT NOT NULL,
    title         VARCHAR(200),
    release_date  DATE,
    UNIQUE (content_id, season_number),
    FOREIGN KEY (content_id) REFERENCES content(content_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE episodes (
    episode_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
    season_id        BIGINT NOT NULL,
    episode_number   SMALLINT NOT NULL,
    title            VARCHAR(300) NOT NULL,
    description      TEXT,
    duration_minutes INT NOT NULL,
    release_date     DATE,
    video_url        VARCHAR(500),
    UNIQUE (season_id, episode_number),
    FOREIGN KEY (season_id) REFERENCES seasons(season_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE people (
    person_id   BIGINT AUTO_INCREMENT PRIMARY KEY,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    birth_date  DATE,
    bio         TEXT,
    photo_url   VARCHAR(500),
    nationality CHAR(2)
) ENGINE=InnoDB;

CREATE TABLE content_people (
    content_id     BIGINT NOT NULL,
    person_id      BIGINT NOT NULL,
    role           VARCHAR(20) NOT NULL,
    character_name VARCHAR(200),
    billing_order  SMALLINT,
    PRIMARY KEY (content_id, person_id, role),
    FOREIGN KEY (content_id) REFERENCES content(content_id) ON DELETE CASCADE,
    FOREIGN KEY (person_id) REFERENCES people(person_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE watch_history (
    watch_id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    profile_id         BIGINT NOT NULL,
    content_id         BIGINT NOT NULL,
    episode_id         BIGINT,
    started_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    progress_percent   DECIMAL(5,2) DEFAULT 0.00,
    completed          BOOLEAN NOT NULL DEFAULT FALSE,
    FOREIGN KEY (profile_id) REFERENCES profiles(profile_id) ON DELETE CASCADE,
    FOREIGN KEY (content_id) REFERENCES content(content_id) ON DELETE CASCADE,
    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id) ON DELETE SET NULL
) ENGINE=InnoDB;

CREATE TABLE my_list (
    list_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    profile_id  BIGINT NOT NULL,
    content_id  BIGINT NOT NULL,
    added_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sort_order  INT DEFAULT 0,
    UNIQUE (profile_id, content_id),
    FOREIGN KEY (profile_id) REFERENCES profiles(profile_id) ON DELETE CASCADE,
    FOREIGN KEY (content_id) REFERENCES content(content_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE ratings (
    rating_id   BIGINT AUTO_INCREMENT PRIMARY KEY,
    profile_id  BIGINT NOT NULL,
    content_id  BIGINT NOT NULL,
    score       SMALLINT NOT NULL CHECK (score BETWEEN 1 AND 10),
    review_text TEXT,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE (profile_id, content_id),
    FOREIGN KEY (profile_id) REFERENCES profiles(profile_id) ON DELETE CASCADE,
    FOREIGN KEY (content_id) REFERENCES content(content_id) ON DELETE CASCADE
) ENGINE=InnoDB;
