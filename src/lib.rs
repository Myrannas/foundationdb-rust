extern crate foundationdbrs;
extern crate futures;
extern crate failure;
#[macro_use] extern crate failure_derive;

use foundationdbrs as fdb;
mod future;

mod cluster;
mod database;
mod transaction;
mod errors;
mod bindings;

use futures::Future;

#[allow(dead_code)]
pub struct FoundationDB {
    cluster: cluster::Cluster,
    database: database::Database
}

impl FoundationDB {
    pub fn new() -> impl futures::Future<Item=FoundationDB, Error=errors::FoundationError> {
        unsafe {
            fdb::fdb_select_api_version_impl(
                fdb::FDB_API_VERSION as i32,
                fdb::FDB_API_VERSION as i32,
            );

            fdb::fdb_setup_network();
        }

        std::thread::spawn(|| {
            unsafe { fdb::fdb_run_network() }
        });

        println!("Started network");

        cluster::Cluster::with_default_cluster()
            .and_then(|cluster| {
                println!("Started cluster");

                cluster.create_database().map(|database| {
                    println!("Started FoundationDB client");

                    FoundationDB {
                        cluster,
                        database
                    }
                })
            })
    }

    pub fn transaction(&self) -> Result<transaction::Transaction, errors::FoundationError> {
        self.database.create_transaction()
    }
}

impl Drop for FoundationDB {
    fn drop(&mut self) {
        unsafe { fdb::fdb_stop_network() };
    }
}
