use std;
use future;
use errors::{FoundationError, check_return_code};
use foundationdbrs as fdb;
use transaction;

pub struct Database {
    database: *mut fdb::FDBDatabase,
}

pub fn parse(handle: future::FutureHandle) -> Result<Database, FoundationError> {
    unsafe {
        let mut database: *mut fdb::FDBDatabase = std::mem::uninitialized();

        let err = fdb::fdb_future_get_database(handle.future, &mut database);
        check_return_code(err)?;

        Ok(Database {
            database
        })
    }
}

impl Database {
    pub fn create_transaction(&self) -> Result<transaction::Transaction, FoundationError> {
        unsafe {
            let mut transaction: *mut fdb::FDBTransaction = std::mem::uninitialized();
            let err = fdb::fdb_database_create_transaction(self.database, &mut transaction);

            check_return_code(err)?;

            Ok(transaction::new_transaction(transaction))
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        unsafe { fdb::fdb_database_destroy(self.database) }
    }
}