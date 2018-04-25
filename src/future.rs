use foundationdbrs as fdb;
use futures::task;
use std::mem;
use futures::{Async, Future};
use errors::{FoundationError, check_return_code};
type TypeParser<T> = fn(future: FutureHandle) -> Result<T, FoundationError>;

enum FutureState {
    New(FutureHandle),
    Bound(FutureHandle),
    Consumed
}

pub struct FDBFuture<T: Sized> {
    state: FutureState,
    parser: TypeParser<T>,
}

pub struct FutureHandle {
    pub future: *mut fdb::FDBFuture,
}

impl Drop for FutureHandle {
    fn drop(&mut self) {
        unsafe {
            fdb::fdb_future_release_memory(self.future);
        }
    }
}

unsafe impl Send for FutureHandle {}
//
//pub trait BlockableFoundationFuture<T>: Future<Item=T, Error=FoundationError> {
//    fn read(self) -> Result<Self::Item, Self::Error>;
//}
//
//impl<T> BlockableFoundationFuture<T> for Future<Item=T, Error=FoundationError> {
//    fn read(self) -> Result<T, FoundationError> {
//        match self {
//            FDBFuture { .. } => {
//                unsafe {
//                    let err = fdb::fdb_future_block_until_ready(handle.future);
//                    check_return_code(err)?;
//                };
//
//                (self.parser)(handle)
//            },
//            _ =>
//        }
//        match self.state {
//            FutureState::New(handle) => {
//                unsafe {
//                    let err = fdb::fdb_future_block_until_ready(handle.future);
//                    check_return_code(err)?;
//                };
//
//                (self.parser)(handle)
//            }
//            _ => Err(FoundationError::PromiseAlreadyConsumed)
//        }
//
//    }
//}

impl<T> FDBFuture<T> {
    pub fn new(future: *mut fdb::FDBFuture, parser: TypeParser<T>) -> FDBFuture<T> {

        return FDBFuture {
            state: FutureState::New(FutureHandle{ future }),
            parser,
        }
    }
}

unsafe extern "C" fn wake(future: *mut fdb::FDBFuture,
                          callback_parameter:
                          *mut ::std::os::raw::c_void) {
    let task = Box::from_raw(callback_parameter as *mut task::Task);
    task.notify();
}

impl FutureState {
    fn step<T>(self, parser: TypeParser<T>, task: task::Task) -> Result<(Option<T>, FutureState), FoundationError> {
        match self {
            FutureState::New(handle) => {
                let mut waker = Box::new(task);
                let mut waker = Box::into_raw(waker);

                unsafe {
                    let err = fdb::fdb_future_set_callback(handle.future, Some(wake), waker as *mut task::Task as *mut ::std::os::raw::c_void);
                    check_return_code(err)?;
                };

                Ok((None, FutureState::Bound(handle)))
            }
            FutureState::Bound(handle) => {
                let complete = unsafe {
                    fdb::fdb_future_is_ready(handle.future)
                };

                if complete != 0 {
                    unsafe {
                        let err = fdb::fdb_future_get_error(handle.future);
                        check_return_code(err)?;
                    };

                    let result = parser(handle)?;
                    Ok((Some(result), FutureState::Consumed))
                } else {
                    Err(FoundationError::PromiseNotReady)
                }
            },
            FutureState::Consumed => Err(FoundationError::PromiseAlreadyConsumed)
        }
    }
}

impl<T> Future for FDBFuture<T> {
    type Item = T;
    type Error = FoundationError;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        let old_state = mem::replace(&mut self.state, FutureState::Consumed);

        match old_state.step(self.parser, task::current()) {
            Ok((result, next_state)) => {
                mem::replace(&mut self.state, next_state);

                match result {
                    Some(t) => Ok(Async::Ready(t)),
                    None => Ok(Async::NotReady)
                }
            },
            Err(err) => Err(err)
        }
    }
}
