use super::Error;
use postgres::{Client, NoTls};

pub struct PostgresStore<'a> {
    db: &'a Client,
}

impl<'a> PostgresStore<'a> {
    pub fn new(client: &'a Client) -> Result<Self, Error> {
        Ok(Self { db: client })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_connection_pool_works() {
        crate::logger::init_log();
        let database_url = "postgres://hupeng@localhost/sqlx_demo";
        let mut client = Client::connect(database_url, NoTls).unwrap();
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS blocks(
            id SERIAL NOT NULL,
            number numeric NOT NULL,
            hash bytea NOT NULL,
            parent_hash bytea NOT NULL,
            is_orphan boolean DEFAULT false
        )",
            )
            .unwrap();
    }
}
