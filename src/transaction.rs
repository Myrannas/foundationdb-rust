use foundationdbrs as fdb;
use future::{FDBFuture, FutureHandle};
use errors::{FoundationError, check_return_code};
use std;
use futures::Future;

pub struct Transaction {
    transaction: *mut fdb::FDBTransaction,
}

unsafe impl Send for Transaction {}

pub unsafe fn new_transaction(transaction: *mut fdb::FDBTransaction) -> Transaction {
    Transaction {
        transaction,
    }
}

#[allow(dead_code)]
pub struct Value {
    handle: FutureHandle,
    data: &'static [u8],
}

impl Value {
    pub fn val(&self) -> &[u8] {
        self.data
    }
}

pub fn parse_value(handle: FutureHandle) -> Result<Option<Value>, FoundationError> {
    unsafe {
        let mut present: i32 = 0;
        let mut len: i32 = 0;
        let mut data: *const u8 = std::mem::uninitialized();

        let err = fdb::fdb_future_get_value(handle.future, &mut present, &mut data, &mut len);
        check_return_code(err)?;

        if present == 0 {
            Ok(None)
        } else {
            Ok(Some(Value {
                handle,
                data: std::slice::from_raw_parts(data, len as usize),
            }))
        }
    }
}

pub fn parse_empty(handle: FutureHandle) -> Result<(), FoundationError> {
    Ok(())
}

impl Transaction {
    pub fn read(&self, key: &[u8]) -> impl Future<Item=Option<Value>, Error=FoundationError> {
        let future = unsafe {
            fdb::fdb_transaction_get(self.transaction, key.as_ptr(), key.len() as i32, false as i32)
        };

        FDBFuture::new(future, parse_value)
    }

    pub fn write(&self, key: &[u8], value: &[u8]) {
        unsafe {
            fdb::fdb_transaction_set(self.transaction, key.as_ptr(), key.len() as i32, value.as_ptr(), value.len() as i32)
        };
    }

    pub fn commit(self) -> impl Future<Item=i64, Error=FoundationError> {
        let future = unsafe { fdb::fdb_transaction_commit(self.transaction) };

        FDBFuture::new(future, parse_empty).and_then(move |_| {
            unsafe {
                let mut result: i64 = 0;
                let err = fdb::fdb_transaction_get_committed_version(self.transaction, &mut result);
                check_return_code(err)?;
                Ok(result)
            }
        })
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        unsafe { fdb::fdb_transaction_destroy(self.transaction) }
    }
}