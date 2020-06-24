CREATE TABLE IF NOT EXISTS block_digests(
    block_number NUMERIC PRIMARY KEY,
    block_hash bytea UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS transaction_digests(
    id SERIAL PRIMARY KEY,
    tx_hash bytea UNIQUE NOT NULL,
    tx_index INTEGER NOT NULL,
    output_count INTEGER NOT NULL,
    block_number NUMERIC NOT NULL REFERENCES block_digests(block_number) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS transaction_inputs(
    id SERIAL PRIMARY KEY,
    transaction_digest_id INTEGER NOT NULL REFERENCES transaction_digests(id) ON DELETE CASCADE,
    previous_tx_hash bytea NOT NULL,
    previous_index INTEGER NOT NULL,
    UNIQUE (previous_tx_hash, previous_index)
);
CREATE TABLE IF NOT EXISTS scripts(
    id SERIAL PRIMARY KEY,
    script_hash bytea UNIQUE NOT NULL,
    code_hash bytea NOT NULL,
    hash_type INTEGER NOT NULL,
    args bytea
);
CREATE INDEX code_hash_hash_type ON scripts (code_hash, hash_type);
CREATE TABLE IF NOT EXISTS transaction_scripts(
    id SERIAL PRIMARY KEY,
    script_type INTEGER NOT NULL,
    io_type INTEGER NOT NULL,
    index INTEGER NOT NULL,
    transaction_digest_id INTEGER NOT NULL REFERENCES transaction_digests(id) ON DELETE CASCADE,
    script_id INTEGER NOT NULL REFERENCES scripts(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS cells(
    id SERIAL PRIMARY KEY,
    capacity NUMERIC NOT NULL,
    lock_script_id INTEGER NOT NULL REFERENCES scripts(id) ON DELETE CASCADE,
    type_script_id INTEGER REFERENCES scripts(id) ON DELETE CASCADE,
    consumed boolean NOT NULL DEFAULT false,
    transaction_digest_id INTEGER NOT NULL REFERENCES transaction_digests(id) ON DELETE CASCADE,
    tx_hash bytea NOT NULL,
    index INTEGER NOT NULL,
    block_number NUMERIC NOT NULL REFERENCES block_digests(block_number) ON DELETE CASCADE,
    tx_index INTEGER NOT NULL,
    UNIQUE (tx_hash, index)
);