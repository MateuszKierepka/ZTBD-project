CREATE TABLE users (
    user_id     BIGSERIAL PRIMARY KEY,
    email       VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    date_of_birth DATE NOT NULL,
    country_code CHAR(2) NOT NULL,
    phone       VARCHAR(20),
    status      VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE profiles (
    profile_id      BIGSERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    name            VARCHAR(50) NOT NULL,
    avatar_url      VARCHAR(500),
    is_kids         BOOLEAN NOT NULL DEFAULT FALSE,
    maturity_rating VARCHAR(10) NOT NULL DEFAULT 'ALL',
    language        VARCHAR(5) NOT NULL DEFAULT 'pl',
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE subscriptions (
    subscription_id BIGSERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    plan_name       VARCHAR(50) NOT NULL,
    price_monthly   DECIMAL(10,2) NOT NULL,
    max_streams     SMALLINT NOT NULL,
    max_resolution  VARCHAR(10) NOT NULL,
    max_profiles    SMALLINT NOT NULL DEFAULT 5,
    status          VARCHAR(20) NOT NULL DEFAULT 'active',
    start_date      DATE NOT NULL,
    end_date        DATE,
    auto_renew      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE payments (
    payment_id      BIGSERIAL PRIMARY KEY,
    subscription_id BIGINT NOT NULL REFERENCES subscriptions(subscription_id) ON DELETE CASCADE,
    amount          DECIMAL(10,2) NOT NULL,
    currency        CHAR(3) NOT NULL DEFAULT 'PLN',
    payment_method  VARCHAR(30) NOT NULL,
    transaction_id  VARCHAR(255) UNIQUE,
    status          VARCHAR(20) NOT NULL,
    paid_at         TIMESTAMP,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE content (
    content_id        BIGSERIAL PRIMARY KEY,
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
    metadata          JSONB,
    created_at        TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE seasons (
    season_id     BIGSERIAL PRIMARY KEY,
    content_id    BIGINT NOT NULL REFERENCES content(content_id) ON DELETE CASCADE,
    season_number SMALLINT NOT NULL,
    title         VARCHAR(200),
    release_date  DATE,
    UNIQUE (content_id, season_number)
);

CREATE TABLE episodes (
    episode_id      BIGSERIAL PRIMARY KEY,
    season_id       BIGINT NOT NULL REFERENCES seasons(season_id) ON DELETE CASCADE,
    episode_number  SMALLINT NOT NULL,
    title           VARCHAR(300) NOT NULL,
    description     TEXT,
    duration_minutes INT NOT NULL,
    release_date    DATE,
    video_url       VARCHAR(500),
    UNIQUE (season_id, episode_number)
);

CREATE TABLE people (
    person_id   BIGSERIAL PRIMARY KEY,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    birth_date  DATE,
    bio         TEXT,
    photo_url   VARCHAR(500),
    nationality CHAR(2)
);

CREATE TABLE content_people (
    content_id     BIGINT NOT NULL REFERENCES content(content_id) ON DELETE CASCADE,
    person_id      BIGINT NOT NULL REFERENCES people(person_id) ON DELETE CASCADE,
    role           VARCHAR(20) NOT NULL,
    character_name VARCHAR(200),
    billing_order  SMALLINT,
    PRIMARY KEY (content_id, person_id, role)
);

CREATE TABLE watch_history (
    watch_id          BIGSERIAL PRIMARY KEY,
    profile_id        BIGINT NOT NULL REFERENCES profiles(profile_id) ON DELETE CASCADE,
    content_id        BIGINT NOT NULL REFERENCES content(content_id) ON DELETE CASCADE,
    episode_id        BIGINT REFERENCES episodes(episode_id) ON DELETE SET NULL,
    started_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    progress_percent  DECIMAL(5,2) DEFAULT 0.00,
    completed         BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE my_list (
    list_id     BIGSERIAL PRIMARY KEY,
    profile_id  BIGINT NOT NULL REFERENCES profiles(profile_id) ON DELETE CASCADE,
    content_id  BIGINT NOT NULL REFERENCES content(content_id) ON DELETE CASCADE,
    added_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    sort_order  INT DEFAULT 0,
    UNIQUE (profile_id, content_id)
);

CREATE TABLE ratings (
    rating_id   BIGSERIAL PRIMARY KEY,
    profile_id  BIGINT NOT NULL REFERENCES profiles(profile_id) ON DELETE CASCADE,
    content_id  BIGINT NOT NULL REFERENCES content(content_id) ON DELETE CASCADE,
    score       SMALLINT NOT NULL CHECK (score BETWEEN 1 AND 10),
    review_text TEXT,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (profile_id, content_id)
);
