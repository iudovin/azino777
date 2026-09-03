-- 1. Схемы
DROP SCHEMA IF EXISTS raw CASCADE;
DROP SCHEMA IF EXISTS dds CASCADE;
DROP SCHEMA IF EXISTS dm CASCADE;

CREATE SCHEMA raw;
CREATE SCHEMA dds;
CREATE SCHEMA dm;

-- 2. Слой RAW (повторяет структуру исходных файлов)
CREATE TABLE IF NOT EXISTS raw.players (
    id                VARCHAR,
    registration_date VARCHAR,
    registration_type VARCHAR,
    country           VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.currency_rates (
    date        VARCHAR,
    currency    VARCHAR,
    rate_to_usd NUMERIC
);

CREATE TABLE IF NOT EXISTS raw.providers_map (
    provider_id   VARCHAR,
    provider_name VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.games_map (
    game_id     VARCHAR,
    game_name   VARCHAR,
    provider_id VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.deposits (
    id           VARCHAR,
    player_id    VARCHAR,
    deposit_date VARCHAR,
    provider_id  VARCHAR,
    amount       NUMERIC,
    currency     VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.withdrawals (
    id              VARCHAR,
    player_id       VARCHAR,
    withdrawal_date VARCHAR,
    provider_id     VARCHAR,
    amount          NUMERIC,
    currency        VARCHAR
);

CREATE TABLE IF NOT EXISTS raw.games (
    id          VARCHAR,
    player_id   VARCHAR,
    game_date   VARCHAR,
    amount      NUMERIC,
    currency    VARCHAR,
    provider_id VARCHAR,
    game_id     VARCHAR
);

-- 3. Слой DDS: DDL со связями и индексами
-- 3.1. Измерения и справочники
CREATE TABLE IF NOT EXISTS dds.dim_providers (
    provider_id   INT PRIMARY KEY,
    provider_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dim_games (
    game_id     INT PRIMARY KEY,
    game_name   VARCHAR(100) NOT NULL,
    provider_id INT REFERENCES dds.dim_providers(provider_id)
);
CREATE INDEX IF NOT EXISTS idx_dds_games_provider ON dds.dim_games(provider_id);

CREATE TABLE IF NOT EXISTS dds.dim_players (
    player_id         INT PRIMARY KEY,
    registration_date DATE,
    registration_type VARCHAR(50),
    country           VARCHAR(10)
);
CREATE INDEX IF NOT EXISTS idx_dds_players_country ON dds.dim_players(country);

-- 3.2. Факты
CREATE TABLE IF NOT EXISTS dds.fct_deposits (
    deposit_id   INT PRIMARY KEY,
    player_id    INT NOT NULL REFERENCES dds.dim_players(player_id),
    deposit_date DATE NOT NULL,
    provider_id  INT REFERENCES dds.dim_providers(provider_id),
    amount       DECIMAL(15, 2) NOT NULL,
    currency     VARCHAR(10) NOT NULL,
    amount_usd   DECIMAL(15, 2) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dds_dep_date ON dds.fct_deposits(deposit_date);
CREATE INDEX IF NOT EXISTS idx_dds_dep_player ON dds.fct_deposits(player_id);

CREATE TABLE IF NOT EXISTS dds.fct_withdrawals (
    withdrawal_id   INT PRIMARY KEY,
    player_id       INT NOT NULL REFERENCES dds.dim_players(player_id),
    withdrawal_date DATE NOT NULL,
    provider_id     INT REFERENCES dds.dim_providers(provider_id),
    amount          DECIMAL(15, 2) NOT NULL,
    currency        VARCHAR(10) NOT NULL,
    amount_usd      DECIMAL(15, 2) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dds_wth_date ON dds.fct_withdrawals(withdrawal_date);
CREATE INDEX IF NOT EXISTS idx_dds_wth_player ON dds.fct_withdrawals(player_id);

CREATE TABLE IF NOT EXISTS dds.fct_games (
    game_transaction_id INT PRIMARY KEY,
    player_id           INT NOT NULL REFERENCES dds.dim_players(player_id),
    game_date           DATE NOT NULL,
    provider_id         INT REFERENCES dds.dim_providers(provider_id),
    game_id             INT REFERENCES dds.dim_games(game_id),
    amount              DECIMAL(15, 2) NOT NULL,
    currency            VARCHAR(10) NOT NULL,
    amount_usd          DECIMAL(15, 2) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dds_games_date ON dds.fct_games(game_date);
CREATE INDEX IF NOT EXISTS idx_dds_games_player ON dds.fct_games(player_id);

COMMIT;