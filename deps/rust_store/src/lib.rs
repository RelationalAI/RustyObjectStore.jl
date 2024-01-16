use futures_util::StreamExt;
use once_cell::sync::Lazy;
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;
use std::error::Error;
use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::{c_char, c_void};
use std::sync::Arc;
use std::time::Duration;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use object_store::{path::Path, ObjectStore};
use object_store::azure::{MicrosoftAzureBuilder, AzureConfigKey};  // TODO aws::AmazonS3Builder

use moka::future::Cache;
use tokio::io::AsyncWriteExt;

// Our global variables needed by our library at runtime. Note that we follow Rust's
// safety rules here by making them immutable with write-exactly-once semantics using
// either Lazy or OnceCell.
static RT: Lazy<Runtime> = Lazy::new(|| tokio::runtime::Runtime::new()
        .expect("could not initialize tokio runtime"));
// A channel (i.e., a queue) where the GET/PUT requests from Julia are placed and where
// our dispatch task pulls requests.
static SQ: OnceCell<async_channel::Sender<Request>> = OnceCell::new();
// The ObjectStore objects contain the context for communicating with a particular
// storage bucket/account, including authentication info. This caches them so we do
// not need to pay the construction cost for each request.
static CLIENTS: Lazy<Cache<u64, Arc<dyn ObjectStore>>> = Lazy::new(|| Cache::new(10));
// Contains configuration items that affect every request globally by default,
// currently includes retry configuration.
static CONFIG: OnceCell<GlobalConfigOptions> = OnceCell::new();

// The result type used for the API functions exposed to Julia. This is used for both
// synchronous errors, e.g. our dispatch channel is full, and for async errors such
// as HTTP connection errors as part of the async Response.
#[repr(C)]
pub enum CResult {
    Uninitialized = -1,
    Ok = 0,
    Error = 1,
    Backoff = 2,
}

// The types used for our internal dispatch mechanism, for dispatching Julia requests
// to our worker task.
enum Request {
    Get(Path, &'static mut [u8], &'static AzureConnection, &'static mut Response, Notifier),
    Put(Path, &'static [u8], &'static AzureConnection, &'static mut Response, Notifier)
}

unsafe impl Send for Request {}


// libuv is how we notify Julia tasks that their async requests are done.
// Note that this will be linked in from the Julia process, we do not try
// to link it while building this Rust lib.
extern "C" {
    fn uv_async_send(cond: *const c_void) -> i32;
}

#[derive(Debug)]
#[repr(C)]
pub struct Notifier {
    handle: *const c_void,
}

impl Notifier {
    fn notify(&self) -> i32 {
        unsafe { uv_async_send(self.handle) }
    }
}

unsafe impl Send for Notifier {}

#[repr(C)]
pub struct AzureConnection {
    account: *const c_char,
    container: *const c_char,
    access_key: *const c_char,
    host: *const c_char,
    sas_token: *const c_char,
    max_retries: usize,     // If 0, will use global config default
    retry_timeout_sec: u64, // If 0, will use global config default
}

#[repr(C)]
pub struct GlobalConfigOptions {
    max_retries: usize,
    retry_timeout_sec: u64,
}

impl AzureConnection {
    fn get_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        let (account, container, key, host, sas_token) = self.as_cstr_tuple();
        hasher.write(account.to_bytes());
        hasher.write(container.to_bytes());
        hasher.write(key.to_bytes());
        hasher.write(host.to_bytes());
        hasher.write(sas_token.to_bytes());
        hasher.write_usize(self.max_retries);
        hasher.write_u64(self.retry_timeout_sec);
        hasher.finish()
    }

    fn as_cstr_tuple(&self) -> (&CStr, &CStr, &CStr, &CStr, &CStr) {
        let account = unsafe { std::ffi::CStr::from_ptr(self.account) };
        let container = unsafe { std::ffi::CStr::from_ptr(self.container) };
        let key = unsafe { std::ffi::CStr::from_ptr(self.access_key) };
        let host = unsafe { std::ffi::CStr::from_ptr(self.host) };
        let sas_token = unsafe { std::ffi::CStr::from_ptr(self.sas_token) };
        (account, container, key, host, sas_token)
    }

    fn to_string_tuple(&self) -> (String, String, String, String, String) {
        let (account, container, key, host, sas_token) = self.as_cstr_tuple();
        (
            account.to_str().unwrap().to_string(),
            container.to_str().unwrap().to_string(),
            key.to_str().unwrap().to_string(),
            host.to_str().unwrap().to_string(),
            sas_token.to_str().unwrap().to_string()
        )
    }
}

unsafe impl Send for AzureConnection {}
unsafe impl Sync for AzureConnection {}

// The type used to give Julia the result of an async request. It will be allocated
// by Julia as part of the request and filled in by Rust.
#[repr(C)]
pub struct Response {
    result: CResult,
    length: usize,
    error_message: *mut i8,
}

unsafe impl Send for Response {}

impl Response {
    fn success(&mut self, length: usize) {
        self.result = CResult::Ok;
        self.length = length;
        self.error_message = std::ptr::null_mut();
    }

    fn _error(&mut self, error_message: impl AsRef<str>) {
        self.result = CResult::Error;
        self.length = 0;
        let c_string = CString::new(error_message.as_ref()).expect("should not have nulls");
        self.error_message = c_string.into_raw();
    }

    fn from_error(&mut self, error: impl std::fmt::Display) {
        self.result = CResult::Error;
        self.length = 0;
        let c_string = CString::new(format!("{}", error)).expect("should not have nulls");
        self.error_message = c_string.into_raw();
    }
}

async fn multipart_get(slice: &'static mut [u8], path: &Path, client: &dyn ObjectStore) -> anyhow::Result<usize, Box<dyn Error>> {
    let part_size: usize = 8 * 1024 * 1024; // 8MB
    let result = client.head(&path).await?;
    if result.size > slice.len() {
        return Err("Supplied buffer was too small".into());
    }

    // If the object size happens to be smaller than part_size,
    // then we will end up doing a single range get of the whole
    // object.
    let mut parts = result.size / part_size;
    if result.size % part_size != 0 {
        parts += 1;
    }
    let mut part_ranges = Vec::with_capacity(parts);
    for i in 0..(parts-1) {
        part_ranges.push((i*part_size)..((i+1)*part_size));
    }
    // Last part which handles sizes not divisible by part_size
    part_ranges.push(((parts-1)*part_size)..result.size);

    let result_vec = client.get_ranges(&path, &part_ranges).await?;
    let accum = tokio::spawn(async move {
        let mut accum: usize = 0;
        for i in 0..result_vec.len() {
            slice[accum..accum + result_vec[i].len()].copy_from_slice(&result_vec[i]);
            accum += result_vec[i].len();
        }
        accum
    }).await?;

    return Ok(accum);
}

async fn multipart_put(slice: &'static [u8], path: &Path, client: &dyn ObjectStore) -> anyhow::Result<(), Box<dyn Error>> {
    let (multipart_id, mut writer) = client.put_multipart(&path).await?;
    match writer.write_all(slice).await {
        Ok(_) => {
            match writer.flush().await {
                Ok(_) => {
                    writer.shutdown().await?;
                    return Ok(());
                }
                Err(e) => {
                    client.abort_multipart(&path, &multipart_id).await?;
                    return Err(Box::new(e));
                }
            }
        }
        Err(e) => {
            client.abort_multipart(&path, &multipart_id).await?;
            return Err(Box::new(e));
        }
    };
}

async fn connect(connection: &AzureConnection) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let (account, container, access_key, host, sas_token) = connection.to_string_tuple();
    let max_retries = if connection.max_retries > 0 { connection.max_retries } else
                            { CONFIG.get().unwrap().max_retries };
    let retry_timeout = if connection.retry_timeout_sec > 0
                            { Duration::from_secs(connection.retry_timeout_sec) }
                        else
                            { Duration::from_secs(CONFIG.get().unwrap().retry_timeout_sec) };
    let mut azure = MicrosoftAzureBuilder::new()
        .with_account(account)
        .with_container_name(container)
        .with_retry(object_store::RetryConfig {
            max_retries: max_retries,
            retry_timeout: retry_timeout,
            ..Default::default()
        })
        .with_client_options(object_store::ClientOptions::new()
            .with_timeout(std::time::Duration::from_secs(20))
            .with_connect_timeout(std::time::Duration::from_secs(10))
        );
    if access_key != "" {
        azure = azure.with_access_key(access_key);
    }

    if sas_token != "" {
        azure = azure.with_config(AzureConfigKey::SasKey, sas_token);
    }

    if host.len() > 0 {
        tracing::debug!("host = {}", host);
        let mut url = url::Url::parse(&host)?;
        url.set_path("");
        std::env::set_var("AZURITE_BLOB_STORAGE_URL", url.as_str());
        azure = azure.with_allow_http(true)
            .with_use_emulator(true)
            .with_client_options(object_store::ClientOptions::new()
                .with_timeout(std::time::Duration::from_secs(20))
                .with_connect_timeout(std::time::Duration::from_secs(10))
                .with_allow_invalid_certificates(true)
            );
    }
    let azure = azure.build()?;

    let client: Arc<dyn ObjectStore> = Arc::new(azure);

    Ok(client)
}

#[no_mangle]
pub extern "C" fn start(config: GlobalConfigOptions) -> CResult {
    match CONFIG.set(config) {
        Ok(_) => {},
        Err(_) => {
            tracing::warn!("Tried to start() runtime multiple times!");
            return CResult::Error;
        }
    }
    tracing_subscriber::fmt::init();

    // Creates our main dispatch task that takes Julia requests from the queue and does the
    // GET or PUT. Note the 'buffer_unordered' call at the end of the map block, which lets
    // requests in the queue be processed concurrently and in any order.
    RT.spawn(async move {
        let (tx, rx) = async_channel::bounded(16 * 1024);
        SQ.set(tx).expect("runtime already started");

        rx.map(|req| {
            async {
                match req {
                    Request::Get(p, slice, connection, response, notifier) => {
                        let client = match CLIENTS.try_get_with(connection.get_hash(), connect(connection)).await {
                            Ok(client) => client,
                            Err(e) => {
                                response.from_error(e);
                                notifier.notify();
                                return;
                            }
                        };

                        // Multipart Get
                        let part_size: usize = 8 * 1024 * 1024; // 8MB
                        if slice.len() > part_size {
                            match multipart_get(slice, &p, &client).await {
                                Ok(accum) => {
                                    response.success(accum);
                                    notifier.notify();
                                    return;
                                }
                                Err(e) => {
                                    tracing::warn!("{}", e);
                                    response.from_error(e);
                                    notifier.notify();
                                    return;
                                }
                            }
                        }

                        // Single part Get
                        match client.get(&p).await {
                            Ok(result) => {
                                let chunks = result.into_stream().collect::<Vec<_>>().await;
                                if let Some(Err(e)) = chunks.iter().find(|result| result.is_err()) {
                                        tracing::warn!("{}", e);
                                        response.from_error(e);
                                        notifier.notify();
                                        return;
                                }
                                tokio::spawn(async move {
                                    let mut received_bytes = 0;
                                    let mut failed = false;
                                    for chunk in chunks {
                                        let chunk = match chunk {
                                            Ok(c) => c,
                                            Err(_e) => {
                                                unreachable!("checked for errors before");
                                            }
                                        };
                                        let len = chunk.len();

                                        if received_bytes + len > slice.len() {
                                            response._error("Supplied buffer was too small");
                                            failed = true;
                                            break;
                                        }

                                        slice[received_bytes..(received_bytes + len)].copy_from_slice(&chunk);
                                        received_bytes += len;
                                    }
                                    if !failed {
                                        response.success(received_bytes);
                                    }
                                    notifier.notify();
                                });
                            }
                            Err(e) => {
                                tracing::warn!("{}", e);
                                response.from_error(e);
                                notifier.notify();
                                return;
                            }
                        }
                    }
                    Request::Put(p, slice, connection, response, notifier) => {
                        let client = match CLIENTS.try_get_with(connection.get_hash(), connect(connection)).await {
                            Ok(client) => client,
                            Err(e) => {
                                response.from_error(e);
                                notifier.notify();
                                return;
                            }
                        };
                        let len = slice.len();
                        if len < 8 * 1024 * 1024 {
                            match client.put(&p, slice.into()).await {
                                Ok(_) => {
                                    response.success(len);
                                    notifier.notify();
                                    return;
                                }
                                Err(e) => {
                                    tracing::warn!("{}", e);
                                    response.from_error(e);
                                    notifier.notify();
                                    return;
                                }
                            }
                        } else {
                            match multipart_put(slice, &p, &client).await {
                                Ok(_) => {
                                    response.success(len);
                                    notifier.notify();
                                    return;
                                }
                                Err(e) => {
                                    tracing::warn!("{}", e);
                                    response.from_error(e);
                                    notifier.notify();
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }).buffer_unordered(512).for_each(|_| async {}).await;
    });
    CResult::Ok
}

#[no_mangle]
pub extern "C" fn perform_get(
    path: *const c_char,
    buffer: *mut u8,
    size: usize,
    connection: *const AzureConnection,
    response: *mut Response,
    handle: *const c_void
) -> CResult {
    let response = unsafe { &mut (*response) };
    response.result = CResult::Uninitialized;
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path: Path = path.to_str().expect("invalid utf8").try_into().unwrap();
    let slice = unsafe { std::slice::from_raw_parts_mut(buffer, size) };
    let connection = unsafe { & (*connection) };
    let notifier = Notifier { handle };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::Get(path, slice, connection, response, notifier)) {
                Ok(_) => CResult::Ok,
                Err(async_channel::TrySendError::Full(_)) => {
                    CResult::Backoff
                }
                Err(async_channel::TrySendError::Closed(_)) => {
                    CResult::Error
                }
            }
        }
        None => {
            return CResult::Error;
        }
    }
}

#[no_mangle]
pub extern "C" fn perform_put(
    path: *const c_char,
    buffer: *const u8,
    size: usize,
    connection: *const AzureConnection,
    response: *mut Response,
    handle: *const c_void
) -> CResult {
    let response = unsafe { &mut (*response) };
    response.result = CResult::Uninitialized;
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path: Path = path.to_str().expect("invalid utf8").try_into().unwrap();
    let slice = unsafe { std::slice::from_raw_parts(buffer, size) };
    let connection = unsafe { & (*connection) };
    let notifier = Notifier { handle };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::Put(path, slice, connection, response, notifier)) {
                Ok(_) => CResult::Ok,
                Err(async_channel::TrySendError::Full(_)) => {
                    CResult::Backoff
                }
                Err(async_channel::TrySendError::Closed(_)) => {
                    CResult::Error
                }
            }
        }
        None => {
            return CResult::Error;
        }
    }
}

#[no_mangle]
pub extern "C" fn destroy_cstring(string: *mut c_char) -> CResult {
    let string = unsafe { std::ffi::CString::from_raw(string) };
    drop(string);
    CResult::Ok
}
