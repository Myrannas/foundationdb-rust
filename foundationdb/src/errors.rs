use foundationdbrs as fdb;
use std::ffi::CStr;

#[derive(Fail, Debug)]
pub enum FoundationError {
    #[fail(display = "Promise was already consumed")]
    PromiseAlreadyConsumed,
    #[fail(display = "Attempted to consume promise before ready")]
    PromiseNotReady,
    #[fail(display = "FoundationDB error: {}", _0)]
    DatabaseError(&'static str),
}

pub unsafe fn check_return_code(error: fdb::fdb_error_t) -> Result<(), FoundationError> {
    if error == 0 {
        return Ok(());
    }

    let error_pointer = fdb::fdb_get_error(error);
    let err = CStr::from_ptr(error_pointer).to_str().unwrap();

    Err(FoundationError::DatabaseError(err))
}