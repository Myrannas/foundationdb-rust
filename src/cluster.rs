use foundationdbrs as fdb;
use futures;
use future;
use errors::{ FoundationError, check_return_code};
use database;
use std;
use std::ptr::null;
use std::ffi::CStr;

pub struct Cluster {
    cluster: *mut fdb::FDBCluster,
}

const DB_NAME: &str = "DB";

fn parse(result: future::FutureHandle) -> Result<Cluster, FoundationError> {
    unsafe {
        let mut cluster: *mut fdb::FDBCluster = std::mem::uninitialized();
        let err = fdb::fdb_future_get_cluster(result.future, &mut cluster);

        check_return_code(err)?;

        Ok(Cluster {
            cluster
        })
    }
}

unsafe impl Send for Cluster {}

impl Cluster {
//    pub fn with_cluster(path: &[u8]) -> future::FDBFuture<Cluster> {
//        let cluster_path = CStr::from_bytes_with_nul(path).unwrap();
//        let result = unsafe { fdb::fdb_create_cluster(cluster_path) };
//
//        future::FDBFuture::new(result, parse)
//    }

    pub fn with_default_cluster() -> impl futures::Future<Item=Cluster, Error=FoundationError> {
        let result = unsafe { fdb::fdb_create_cluster(null()) };

        future::FDBFuture::new(result, parse)
    }

    pub fn create_database(&self) -> impl futures::Future<Item=database::Database, Error=FoundationError> {
        let result = unsafe { fdb::fdb_cluster_create_database(self.cluster, DB_NAME.as_bytes().as_ptr(), DB_NAME.len() as i32) };

        future::FDBFuture::new(result, database::parse)
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        unsafe { fdb::fdb_cluster_destroy(self.cluster) };
    }
}