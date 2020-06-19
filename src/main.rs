// use crate::indexer::{Indexer, KvIndexer};
// use crate::service::Service;
// use crate::store::RocksdbStore;
// use clap::{App, Arg};
// use futures::Future;
// use hyper::rt;
// use jsonrpc_core_client::transports::http;

// fn main() {
//     env_logger::Builder::from_default_env()
//         .format_timestamp(Some(env_logger::fmt::TimestampPrecision::Millis))
//         .init();
//     let matches = App::new("ckb indexer")
//         .arg(
//             Arg::with_name("ckb_uri")
//                 .short("c")
//                 .help("CKB rpc http service uri, default 127.0.0.1:8114")
//                 .takes_value(true),
//         )
//         .arg(
//             Arg::with_name("listen_uri")
//                 .short("l")
//                 .help("Indexer rpc http service listen address, default 127.0.0.1:8116")
//                 .takes_value(true),
//         )
//         .arg(
//             Arg::with_name("store_path")
//                 .short("s")
//                 .help("Sets the indexer store path to use")
//                 .required(true)
//                 .takes_value(true),
//         )
//         .get_matches();

//     let store_path = matches.value_of("store_path").expect("required arg");
//     let rocksdb = RocksdbStore::new(store_path);
//     let kv_indexer = Indexer::<RocksdbStore>::new(rocksdb, 100, 1000);
//     let service = Service::<KvIndexer>::new(
//         kv_indexer,
//         matches.value_of("listen_uri").unwrap_or("127.0.0.1:8116"),
//         std::time::Duration::from_secs(2),
//     );
//     let rpc_server = service.start();

//     rt::run(rt::lazy(move || {
//         let uri = format!(
//             "http://{}",
//             matches.value_of("ckb_uri").unwrap_or("127.0.0.1:8114")
//         );

//         http::connect(&uri)
//             .and_then(move |client| {
//                 service.poll(client);
//                 Ok(())
//             })
//             .map_err(|e| {
//                 println!("Error: {:?}", e);
//             })
//     }));

//     rpc_server.close();
// }

fn main() {}
