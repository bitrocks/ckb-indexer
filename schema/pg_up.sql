CREATE TABLE IF NOT EXISTS block_digests(
    id SERIAL PRIMARY KEY,
    block_hash bytea UNIQUE NOT NULL,
    block_number NUMERIC NOT NULL,
    parent_hash bytea NOT NULL
);
CREATE TABLE IF NOT EXISTS transaction_digests(
    id SERIAL PRIMARY KEY,
    tx_hash bytea UNIQUE NOT NULL,
    block_id INTEGER REFERENCES block_digests(id)
);
CREATE TABLE IF NOT EXISTS scripts(
    id SERIAL PRIMARY KEY,
    script_hash bytea UNIQUE NOT NULL,
    code_hash bytea NOT NULL,
    hash_type INTEGER NOT NULL,
    args bytea
);
CREATE TABLE IF NOT EXISTS cells(
    id SERIAL PRIMARY KEY,
    capacity NUMERIC NOT NULL,
    lock_script_id INTEGER REFERENCES scripts(id) NOT NULL,
    type_script_id INTEGER REFERENCES scripts(id),
    consumed boolean NOT NULL DEFAULT false,
    tx_id INTEGER REFERENCES transaction_digests(id),
    tx_hash bytea NOT NULL,
    index INTEGER NOT NULL,
    block_number NUMERIC NOT NULL,
    UNIQUE (tx_hash, index)
);
CREATE TABLE IF NOT EXISTS transaction_inputs(
    id SERIAL PRIMARY KEY,
    tx_id INTEGER REFERENCES transaction_digests(id),
    tx_hash bytea NOT NULL,
    index INTEGER NOT NULL,
    UNIQUE (tx_hash, index)
);
