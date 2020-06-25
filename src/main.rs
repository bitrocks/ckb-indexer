use async_std::task;
use ckb_indexer::service::Service;
use ckb_indexer::sql_service::SqlService;
use ckb_indexer::store::RocksdbStore;
use clap::{App, Arg, SubCommand};
use futures::Future;
use hyper::rt;
use jsonrpc_core_client::transports::http;

fn main() {
    env_logger::Builder::from_default_env()
        .format_timestamp(Some(env_logger::fmt::TimestampPrecision::Millis))
        .init();
    let matches = App::new("ckb indexer")
        .arg(
            Arg::with_name("ckb_uri")
                .short("c")
                .help("CKB rpc http service uri, default 127.0.0.1:8114")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("listen_uri")
                .short("l")
                .help("Indexer rpc http service listen address, default 127.0.0.1:8116")
                .takes_value(true),
        )
        .subcommand(
            SubCommand::with_name("rocksdb")
                .about("Config for rocksdb backend")
                .arg(
                    Arg::with_name("store_path")
                        .short("s")
                        .help("Sets the indexer store path to use")
                        .required(true)
                        .takes_value(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("postgresql")
                .about("Config for postgresql backend")
                .arg(
                    Arg::with_name("database_url")
                        .short("d")
                        .help("Set the sql indexer database url")
                        .required(true)
                        .takes_value(true),
                ),
        )
        .get_matches();

    match matches.subcommand_name() {
        Some("rocksdb") => {
            let sub_rocksdb = matches.subcommand_matches("rocksdb").unwrap();
            let store_path = sub_rocksdb.value_of("store_path").expect("required arg");
            let service = Service::<RocksdbStore>::new(
                store_path,
                matches.value_of("listen_uri").unwrap_or("127.0.0.1:8116"),
                std::time::Duration::from_secs(2),
            );
            let rpc_server = service.start();
            rt::run(rt::lazy(move || {
                let uri = format!(
                    "http://{}",
                    matches.value_of("ckb_uri").unwrap_or("127.0.0.1:8114")
                );
                http::connect(&uri)
                    .and_then(move |client| {
                        service.poll(client);
                        Ok(())
                    })
                    .map_err(|e| {
                        println!("Error: {:?}", e);
                    })
            }));
            rpc_server.close();
        }
        Some("postgresql") => {
            let sub_postgresql = matches.subcommand_matches("postgresql").unwrap();
            let database_url = sub_postgresql
                .value_of("database_url")
                .expect("required arg")
                .to_owned();
            let uri = format!(
                "http://{}",
                matches.value_of("ckb_uri").unwrap_or("127.0.0.1:8114")
            );
            let listen_uri = matches
                .value_of("listen_uri")
                .unwrap_or("127.0.0.1:8116")
                .to_owned();

            rt::run(rt::lazy(move || {
                http::connect(&uri)
                    .and_then(move |client| {
                        task::block_on(async {
                            let sql_service = SqlService::new(
                                &database_url,
                                &listen_uri,
                                std::time::Duration::from_secs(2),
                            )
                            .await
                            .unwrap();
                            sql_service.poll(client).await.unwrap();
                        });
                        Ok(())
                    })
                    .map_err(|e| println!("Error: {:?}", e))
            }));
        }
        Some(_) => {}
        None => {}
    }
}
