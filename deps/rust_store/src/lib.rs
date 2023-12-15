use anyhow::anyhow;
use futures_util::StreamExt;
use once_cell::sync::Lazy;
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;
use std::panic;
use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::{c_char, c_void};
use std::sync::{Once, Arc, atomic::{AtomicUsize, Ordering}};

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use object_store::{path::Path, ObjectStore};
use object_store::azure::MicrosoftAzureBuilder;  // TODO aws::AmazonS3Builder

use moka::future::Cache;

static RT: Lazy<Runtime> = Lazy::new(|| tokio::runtime::Runtime::new()
        .expect("could not initialize tokio runtime"));
static SQ: OnceCell<async_channel::Sender<Request>> = OnceCell::new();
static CLIENTS: Lazy<Cache<u64, Arc<dyn ObjectStore>>> = Lazy::new(|| Cache::new(10));
static CONFIG: OnceCell<GlobalConfigOptions> = OnceCell::new();
static INIT: Once = Once::new();

#[repr(C)]
pub enum CResult {
    Uninitialized = -1,
    Ok = 0,
    Error = 1,
    Backoff = 2,
}

enum Request {
    Get(Path, &'static mut [u8], &'static AzureCredentials, &'static mut Response, Notifier),
    Put(Path, &'static [u8], &'static AzureCredentials, &'static mut Response, Notifier)
}

unsafe impl Send for Request {}

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
pub struct AzureCredentials {
    account: *const c_char,
    container: *const c_char,
    key: *const c_char,
    host: *const c_char
}

#[repr(C)]
pub struct GlobalConfigOptions {
    max_retries: usize,
    retry_timeout_sec: u64
}

impl AzureCredentials {
    fn get_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        let (account, container, key, host) = self.as_cstr_tuple();
        hasher.write(account.to_bytes());
        hasher.write(container.to_bytes());
        hasher.write(key.to_bytes());
        hasher.write(host.to_bytes());
        hasher.finish()
    }

    fn as_cstr_tuple(&self) -> (&CStr, &CStr, &CStr, &CStr) {
        let account = unsafe { std::ffi::CStr::from_ptr(self.account) };
        let container = unsafe { std::ffi::CStr::from_ptr(self.container) };
        let key = unsafe { std::ffi::CStr::from_ptr(self.key) };
        let host = unsafe { std::ffi::CStr::from_ptr(self.host) };
        (account, container, key, host)
    }

    fn to_string_tuple(&self) -> (String, String, String, String) {
        let (account, container, key, host) = self.as_cstr_tuple();
        (
            account.to_str().unwrap().to_string(),
            container.to_str().unwrap().to_string(),
            key.to_str().unwrap().to_string(),
            host.to_str().unwrap().to_string()
        )
    }
}

unsafe impl Send for AzureCredentials {}
unsafe impl Sync for AzureCredentials {}

#[repr(C)]
pub struct Response {
    result: CResult,
    length: usize,
    error_message: *mut i8
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

pub async fn connect_and_test(credentials: &AzureCredentials) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let (account, container, key, host) = credentials.to_string_tuple();
    let mut azure = MicrosoftAzureBuilder::new()
        .with_account(account)
        .with_container_name(container)
        .with_access_key(key)
        .with_retry(object_store::RetryConfig {
            max_retries: CONFIG.get().unwrap().max_retries,
            retry_timeout: std::time::Duration::from_secs(CONFIG.get().unwrap().retry_timeout_sec), ..Default::default() })
        .with_client_options(object_store::ClientOptions::new()
            .with_timeout(std::time::Duration::from_secs(20))
            .with_connect_timeout(std::time::Duration::from_secs(10))
        );

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

    let ping_path: Path = "_this_file_does_not_exist".try_into().unwrap();
    match client.get(&ping_path).await {
        Ok(_) | Err(object_store::Error::NotFound { .. }) => {},
        Err(e) => {
            return Err(anyhow!("failed to check store client connection: {}", e));
        }
    }

    Ok(client)
}

#[no_mangle]
pub extern "C" fn start(panic_handler: unsafe extern "C" fn()->c_void, config: GlobalConfigOptions) -> CResult {
    let default_panic = std::panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        //tracing::warn!("{:?}", info);
        unsafe{
            let _ = panic_handler();
        }
        // We don't expect the panic_handler callback to return to us, but if it does
        // for some reason we should invoke the default handler to crash instead of
        // continuing in an undefined state
        default_panic(info);
    }));

    let _ = CONFIG.set(config);
    // Avoid panic from multiple initialization of the tracing subscriber
    INIT.call_once(|| {
        tracing_subscriber::fmt::init();
    });

    RT.spawn(async move {
        let (tx, rx) = async_channel::bounded(16 * 1024);
        SQ.set(tx).expect("runtime already started");

        let bytes_sent = Arc::new(AtomicUsize::new(0));

        {
            let bytes_sent = Arc::clone(&bytes_sent);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));

                let mut last_bytes = 0;
                loop {
                    interval.tick().await;
                    let bytes = bytes_sent.load(Ordering::Relaxed);
                    // tracing::info!("BW = {:.3} MiB/s", (bytes as f64 / 1024.0 / 1024.0) / start.elapsed().as_secs_f64());
                    tracing::trace!("Instant BW = {:.3} MiB/s", ((bytes - last_bytes) as f64 / 1024.0 / 1024.0));
                    last_bytes = bytes;
                }
            });
        }

        rx.map(|req| {
            async {
                match req {
                    Request::Get(p, slice, credentials, response, notifier) => {
                        let client = match CLIENTS.try_get_with(credentials.get_hash(), connect_and_test(credentials)).await {
                            Ok(client) => client,
                            Err(e) => {
                                response.from_error(e);
                                notifier.notify();
                                return;
                            }
                        };
                        let mut tries = 10;
                        loop {
                            match client.get(&p).await {
                                Ok(result) => {
                                    let chunks = result.into_stream().collect::<Vec<_>>().await;
                                    if let Some(Err(e)) = chunks.iter().find(|result| result.is_err()) {
                                        if tries > 0 {
                                            tracing::trace!("error while fetching a chunk, retrying: {}", e);
                                            tries -= 1;
                                            continue;
                                        } else {
                                            tracing::debug!("{}", e);
                                            response.from_error(e);
                                            notifier.notify();
                                            break;
                                        }
                                    }
                                    {
                                        let bytes_sent = Arc::clone(&bytes_sent);
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
                                            bytes_sent.fetch_add(received_bytes, Ordering::AcqRel);
                                            if !failed {
                                                response.success(received_bytes);
                                            }
                                            notifier.notify();
                                        });
                                    }
                                    break;
                                }
                                Err(ref err @ object_store::Error::Generic { .. }) => {
                                    if tries > 0 {
                                        tries -= 1;
                                        tracing::trace!("generic error, retrying: {}", err);
                                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                        continue;
                                    }

                                    tracing::debug!("{}", err);
                                    response.from_error(err);
                                    notifier.notify();
                                    break;
                                }
                                Err(e) => {
                                    tracing::debug!("{}", e);
                                    response.from_error(e);
                                    notifier.notify();
                                    break;
                                }
                            }
                        }
                    }
                    Request::Put(p, slice, credentials, response, notifier) => {
                        let client = match CLIENTS.try_get_with(credentials.get_hash(), connect_and_test(credentials)).await {
                            Ok(client) => client,
                            Err(e) => {
                                response.from_error(e);
                                notifier.notify();
                                return;
                            }
                        };
                        let len = slice.len();
                        let mut tries = 10;
                        loop {
                            match client.put(&p, slice.into()).await {
                                Ok(_) => {
                                    bytes_sent.fetch_add(len, Ordering::AcqRel);
                                    response.success(len);
                                    notifier.notify();
                                    break;
                                }
                                Err(ref err @ object_store::Error::Generic { .. }) => {
                                    if tries > 0 {
                                        tries -= 1;
                                        tracing::trace!("generic error, retrying: {}", err);
                                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                        continue;
                                    }

                                    tracing::debug!("{}", err);
                                    response.from_error(err);
                                    notifier.notify();
                                    break;
                                }
                                Err(e) => {
                                    tracing::debug!("{}", e);
                                    response.from_error(e);
                                    notifier.notify();
                                    break;
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
    credentials: *const AzureCredentials,
    response: *mut Response,
    handle: *const c_void
) -> CResult {
    let response = unsafe { &mut (*response) };
    response.result = CResult::Uninitialized;
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path: Path = path.to_str().expect("invalid utf8").try_into().unwrap();
    let slice = unsafe { std::slice::from_raw_parts_mut(buffer, size) };
    let credentials = unsafe { & (*credentials) };
    let notifier = Notifier { handle };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::Get(path, slice, credentials, response, notifier)) {
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
    credentials: *const AzureCredentials,
    response: *mut Response,
    handle: *const c_void
) -> CResult {
    let response = unsafe { &mut (*response) };
    response.result = CResult::Uninitialized;
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path: Path = path.to_str().expect("invalid utf8").try_into().unwrap();
    let slice = unsafe { std::slice::from_raw_parts(buffer, size) };
    let credentials = unsafe { & (*credentials) };
    let notifier = Notifier { handle };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::Put(path, slice, credentials, response, notifier)) {
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
