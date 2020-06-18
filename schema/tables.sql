CREATE TABLE IF NOT EXISTS blocks(
    id SERIAL NOT NULL,
    number numeric NOT NULL,
    hash bytea NOT NULL,
    parent_hash bytea NOT NULL,

    is_orphan boolean DEFAULT false,
);

CREATE TABLE IF NOT EXISTS transactions(
    id SERIAL NOT NULL,
    hash bytea NOT NULL,

    block_id INTEGER REFERENCES blocks(id),
);

CREATE TABLE IF NOT EXISTS cell_outputs(
    id SERIAL NOT NULL,
    capacity numeric NOT NULL,
    lock_script_hash bytea NOT NULL,
    type_script_hash bytea,

    is_live boolean NOT NULL DEFAULT true,

    tx_id INTEGER REFERENCES transactions(id),
    tx_hash bytea NOT NULL,
    output_index
);

CREATE TABLE IF NOT EXISTS cell_inputs(
    id SERIAL NOT NULL,
    since numeric NOT NULL,
    
    tx_id INTEGER REFERENCES transactions(id),
    tx_hash bytea NOT NULL,
    input_index numeric NOT NULL,
);

CREATE TABLE IF NOT EXISTS scripts {
    id SERIAL NOT NULL,
    script_hash bytea NOT NULL,
    code_hash bytea NOT NULL,
    hash_type bytea NOT NULL,
    args bytea,
}