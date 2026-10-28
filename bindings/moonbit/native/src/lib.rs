// Copyright 2026 Nowledge contributors
// SPDX-License-Identifier: Apache-2.0
//
// First-class MoonBit native adapter. Exports the C calling convention
// directly from Rust and depends only on the HawDB engine crate.
// MoonBit allocations use the native runtime contract in moonbit.h.

use std::collections::BTreeMap;
use std::ffi::{c_int, c_void};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::ptr;
use std::slice;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use base64::Engine;
use hawdb::{
    DatabaseReadTransaction, EmbeddedQueryError, HawDBEmbedded, HawDBEmbeddedOpenOptions,
    HawDBError, QueryOutput, Uuid, Value,
};
use serde_json::{json, Value as Json};

unsafe extern "C" {
    fn moonbit_make_bytes(length: i32, init: c_int) -> *mut u8;
    fn moonbit_make_external_object(
        finalize: Option<extern "C" fn(*mut c_void)>,
        payload_size: u32,
    ) -> *mut c_void;
}

// Shared ownership retains engine state until all transactions finish.
struct SharedDatabase {
    engine: Mutex<HawDBEmbedded>,
    closed: AtomicBool,
}

fn owned_database(engine: HawDBEmbedded) -> *mut Arc<SharedDatabase> {
    Box::into_raw(Box::new(Arc::new(SharedDatabase {
        engine: Mutex::new(engine),
        closed: AtomicBool::new(false),
    })))
}

#[repr(C)]
pub struct Handle {
    db: *mut Arc<SharedDatabase>,
    error: *mut String,
}

type Failure = (String, String);

fn fail(kind: &str, message: impl Into<String>) -> Failure {
    (kind.to_owned(), message.into())
}

fn variant_name(value: &impl std::fmt::Debug) -> String {
    let value = format!("{value:?}");
    let end = value.find(['(', ' ', '{']).unwrap_or(value.len());
    let mut result = String::new();
    for (i, ch) in value[..end].chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_ascii_lowercase());
        } else {
            result.push(ch);
        }
    }
    result
}

fn engine_error(e: &HawDBError) -> Failure {
    fail(&variant_name(e), e.to_string())
}

fn query_error(e: &EmbeddedQueryError) -> Failure {
    let kind = match e {
        EmbeddedQueryError::Database(inner) => variant_name(inner),
        EmbeddedQueryError::Admission(_) => "admission".to_owned(),
        EmbeddedQueryError::Stopped(_) => "stopped".to_owned(),
    };
    (kind, e.to_string())
}

fn error_json((kind, message): Failure) -> String {
    json!({"kind":kind,"message":message}).to_string()
}

fn clear_error(handle: *mut Handle) {
    if handle.is_null() {
        return;
    }
    unsafe {
        let old = (*handle).error;
        (*handle).error = ptr::null_mut();
        if !old.is_null() {
            drop(Box::from_raw(old));
        }
    }
}

fn save_error(handle: *mut Handle, message: String) {
    clear_error(handle);
    if !handle.is_null() {
        unsafe { (*handle).error = Box::into_raw(Box::new(message)) };
    }
}

fn close_db(handle: *mut Handle) {
    if !handle.is_null() {
        unsafe {
            let old = (*handle).db;
            (*handle).db = ptr::null_mut(); // Make repeated close safe.
            if !old.is_null() {
                let shared = Box::from_raw(old);
                shared.closed.store(true, Ordering::Release);
                drop(shared);
            }
        }
    }
}

extern "C" fn finalize(payload: *mut c_void) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let handle = payload.cast::<Handle>();
        close_db(handle);
        clear_error(handle);
        // MoonBit runtime frees the GC object itself.
    }));
}

fn allocate_handle() -> *mut Handle {
    let ptr = unsafe {
        moonbit_make_external_object(Some(finalize), std::mem::size_of::<Handle>() as u32)
    }
    .cast::<Handle>();
    if !ptr.is_null() {
        unsafe {
            ptr::write(
                ptr,
                Handle {
                    db: ptr::null_mut(),
                    error: ptr::null_mut(),
                },
            )
        };
    }
    ptr
}

fn to_moonbit_bytes(data: &[u8]) -> *mut u8 {
    let Ok(len) = i32::try_from(data.len()) else {
        return unsafe { moonbit_make_bytes(0, 0) };
    };
    let out = unsafe { moonbit_make_bytes(len, 0) };
    if !out.is_null() && len > 0 {
        unsafe { ptr::copy_nonoverlapping(data.as_ptr(), out, data.len()) };
    }
    out
}

unsafe fn input<'a>(p: *const u8, len: i32, what: &str) -> Result<&'a str, Failure> {
    if len < 0 || p.is_null() {
        return Err(fail(
            "invalid_argument",
            format!("{what} has an invalid pointer or length"),
        ));
    }
    let bytes = unsafe { slice::from_raw_parts(p, len as usize) };
    std::str::from_utf8(bytes)
        .map_err(|_| fail("invalid_argument", format!("{what} must be valid UTF-8")))
}

fn decode_value(value: &Json) -> Result<Value, Failure> {
    Ok(match value {
        Json::Null => Value::Null,
        Json::Bool(v) => Value::Bool(*v),
        Json::Number(v) => {
            if let Some(i) = v.as_i64() {
                Value::Int(i)
            } else if let Some(f) = v.as_f64() {
                Value::Float(f)
            } else {
                return Err(fail("invalid_argument", "number out of range"));
            }
        }
        Json::String(v) => Value::String(v.clone()),
        Json::Array(values) => {
            Value::List(values.iter().map(decode_value).collect::<Result<_, _>>()?)
        }
        Json::Object(values) => {
            if values.len() == 1 {
                if let Some(v) = values.get("$map") {
                    let entries = v
                        .as_array()
                        .ok_or_else(|| fail("invalid_argument", "$map must be an array"))?;
                    let mut map = BTreeMap::new();
                    for entry in entries {
                        let pair = entry
                            .as_array()
                            .filter(|p| p.len() == 2)
                            .ok_or_else(|| fail("invalid_argument", "$map entry must be a pair"))?;
                        let key = pair[0]
                            .as_str()
                            .ok_or_else(|| fail("invalid_argument", "$map key must be a string"))?;
                        if map
                            .insert(key.to_owned(), decode_value(&pair[1])?)
                            .is_some()
                        {
                            return Err(fail("invalid_argument", "duplicate $map key"));
                        }
                    }
                    return Ok(Value::Map(map));
                }
                if let Some(v) = values.get("$vector") {
                    let numbers = v
                        .as_array()
                        .ok_or_else(|| fail("invalid_argument", "$vector must be an array"))?
                        .iter()
                        .map(|v| {
                            let x = v.as_f64().ok_or_else(|| {
                                fail("invalid_argument", "$vector requires numbers")
                            })?;
                            let coordinate = x as f32;
                            if !coordinate.is_finite() {
                                return Err(fail(
                                    "invalid_argument",
                                    "$vector coordinate is outside finite f32 range",
                                ));
                            }
                            Ok(coordinate)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    return hawdb::Vector::new(&numbers)
                        .map(Value::Vector)
                        .map_err(|e| fail("invalid_argument", e.to_string()));
                }
                if let Some(v) = values.get("$binary") {
                    let bytes = v
                        .as_str()
                        .ok_or_else(|| fail("invalid_argument", "$binary must be a string"))?;
                    return base64::engine::general_purpose::STANDARD
                        .decode(bytes)
                        .map(Value::Binary)
                        .map_err(|e| fail("invalid_argument", e.to_string()));
                }
                if let Some(v) = values.get("$uuid") {
                    let uuid = v
                        .as_str()
                        .ok_or_else(|| fail("invalid_argument", "$uuid must be a string"))?;
                    return uuid
                        .parse::<Uuid>()
                        .map(Value::Uuid)
                        .map_err(|e| fail("invalid_argument", e.to_string()));
                }
            }
            Value::Map(
                values
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), decode_value(value)?)))
                    .collect::<Result<BTreeMap<_, _>, Failure>>()?,
            )
        }
    })
}

fn encode_value(value: &Value) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::Bool(v) => json!(v),
        Value::Int(v) => json!(v),
        Value::Float(v) if v.is_finite() => json!(v),
        Value::Float(_) => Json::Null,
        Value::String(v) => json!(v),
        Value::Binary(v) => json!({"$binary":base64::engine::general_purpose::STANDARD.encode(v)}),
        Value::Uuid(v) => json!({"$uuid":v.to_string()}),
        Value::Vector(v) => json!({"$vector":v.as_ref().coordinates().collect::<Vec<_>>()}),
        Value::List(v) => Json::Array(v.iter().map(encode_value).collect()),
        Value::Map(v) => {
            let reserved = v.len() == 1
                && v.keys().next().is_some_and(|key| {
                    matches!(key.as_str(), "$binary" | "$uuid" | "$vector" | "$map")
                });
            if reserved {
                let pairs: Vec<Json> = v
                    .iter()
                    .map(|(key, value)| {
                        Json::Array(vec![Json::String(key.clone()), encode_value(value)])
                    })
                    .collect();
                json!({"$map": pairs})
            } else {
                Json::Object(
                    v.iter()
                        .map(|(key, value)| (key.clone(), encode_value(value)))
                        .collect(),
                )
            }
        }
    }
}

fn encode_result(result: &QueryOutput) -> String {
    let rows: Vec<_> = result
        .value_rows()
        .map(|row| Json::Array(row.iter().map(encode_value).collect()))
        .collect();
    json!({"columns":result.schema().columns(),"rows":rows}).to_string()
}

fn named_params(data: &str) -> Result<BTreeMap<String, Value>, Failure> {
    let parsed: Json =
        serde_json::from_str(data).map_err(|e| fail("invalid_argument", e.to_string()))?;
    let obj = parsed
        .as_object()
        .ok_or_else(|| fail("invalid_argument", "Cypher params must be an object"))?;
    obj.iter()
        .map(|(key, val)| Ok((key.clone(), decode_value(val)?)))
        .collect()
}

fn sql_params(data: &str) -> Result<Vec<Value>, Failure> {
    let parsed: Json =
        serde_json::from_str(data).map_err(|e| fail("invalid_argument", e.to_string()))?;
    let arr = parsed
        .as_array()
        .ok_or_else(|| fail("invalid_argument", "SQL params must be an array"))?;
    arr.iter().map(decode_value).collect()
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_open(path: *const u8, len: i32, read_only: i32) -> *mut Handle {
    let handle = allocate_handle();
    if handle.is_null() {
        return handle;
    }
    let opened = catch_unwind(AssertUnwindSafe(|| {
        let path = unsafe { input(path, len, "path") }?;
        let mut options = HawDBEmbeddedOpenOptions::new(PathBuf::from(path));
        options.config.read_only = read_only != 0;
        HawDBEmbedded::open_with_options(options).map_err(|e| engine_error(&e))
    }));
    match opened {
        Ok(Ok(db)) => unsafe { (*handle).db = owned_database(db) },
        Ok(Err(e)) => save_error(handle, error_json(e)),
        Err(_) => save_error(handle, error_json(fail("panic", "panic during open"))),
    }
    handle
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_open_in_memory() -> *mut Handle {
    let handle = allocate_handle();
    if handle.is_null() {
        return handle;
    }
    match catch_unwind(AssertUnwindSafe(HawDBEmbedded::open_in_memory)) {
        Ok(db) => unsafe { (*handle).db = owned_database(db) },
        Err(_) => save_error(
            handle,
            error_json(fail("panic", "panic during open_in_memory")),
        ),
    }
    handle
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_is_open(handle: *mut Handle) -> i32 {
    if handle.is_null() || unsafe { (*handle).db }.is_null() {
        0
    } else {
        1
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_close(handle: *mut Handle) {
    let _ = catch_unwind(AssertUnwindSafe(|| close_db(handle)));
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_execute(
    handle: *mut Handle,
    text: *const u8,
    text_len: i32,
    params: *const u8,
    params_len: i32,
    sql: i32,
) -> *mut u8 {
    if handle.is_null() || unsafe { (*handle).db }.is_null() {
        return to_moonbit_bytes(&[]);
    }
    clear_error(handle);
    let result = catch_unwind(AssertUnwindSafe(|| {
        let statement = unsafe { input(text, text_len, "statement") }?;
        let params = unsafe { input(params, params_len, "params") }?;
        let db = unsafe { &*(*handle).db };
        let mut locked = db
            .engine
            .lock()
            .map_err(|_| fail("execution", "database mutex poisoned"))?;
        if db.closed.load(Ordering::Acquire) {
            return Err(fail("closed", "database is closed"));
        }
        if sql != 0 {
            let args = sql_params(params)?;
            let answer = locked
                .database_mut()
                .query_sql_with_params(statement, &args)
                .map_err(|e| engine_error(&e))?;
            Ok::<_, Failure>(encode_result(&answer))
        } else {
            let args = named_params(params)?;
            let answer = locked
                .query_with_params_admitted(statement, &args)
                .map_err(|e| query_error(&e))?;
            Ok::<_, Failure>(encode_result(&answer))
        }
    }));
    match result {
        Ok(Ok(text)) => to_moonbit_bytes(text.as_bytes()),
        Ok(Err(e)) => {
            save_error(handle, error_json(e));
            to_moonbit_bytes(&[])
        }
        Err(_) => {
            save_error(handle, error_json(fail("panic", "panic during execute")));
            to_moonbit_bytes(&[])
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_error(handle: *mut Handle) -> *mut u8 {
    let message = if handle.is_null() || unsafe { (*handle).error }.is_null() {
        error_json(fail("closed", "HawDB handle is closed"))
    } else {
        unsafe { &*(*handle).error }.clone()
    };
    to_moonbit_bytes(message.as_bytes())
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_version() -> *mut u8 {
    to_moonbit_bytes(concat!("hawdb ", env!("CARGO_PKG_VERSION")).as_bytes())
}

// Native transaction ownership is independent of the parent Handle; after
// Database::close, its own Arc remains until snapshot release / rollback.
#[repr(C)]
pub struct TransactionHandle {
    db: *mut Arc<SharedDatabase>,
    read: *mut DatabaseReadTransaction,
    active: bool,
    read_only: bool,
    error: *mut String,
}

fn tx_clear_error(tx: *mut TransactionHandle) {
    if tx.is_null() {
        return;
    }
    unsafe {
        let ptr = (*tx).error;
        (*tx).error = ptr::null_mut();
        if !ptr.is_null() {
            drop(Box::from_raw(ptr));
        }
    }
}

fn tx_set_error(tx: *mut TransactionHandle, value: Failure) {
    tx_clear_error(tx);
    if !tx.is_null() {
        unsafe { (*tx).error = Box::into_raw(Box::new(error_json(value))) };
    }
}

fn tx_release_db(tx: *mut TransactionHandle) {
    if tx.is_null() {
        return;
    }
    unsafe {
        let old = (*tx).db;
        (*tx).db = ptr::null_mut();
        if !old.is_null() {
            drop(Box::from_raw(old));
        }
    }
}

fn tx_abandon(tx: *mut TransactionHandle) -> Result<(), Failure> {
    if tx.is_null() {
        return Ok(());
    }
    let (db_ptr, read, write) = unsafe {
        let tx = &mut *tx;
        if !tx.active {
            tx_release_db(tx);
            return Ok(());
        }
        tx.active = false;
        let read = tx.read;
        tx.read = ptr::null_mut();
        (tx.db, read, !tx.read_only)
    };
    if !read.is_null() {
        unsafe { drop(Box::from_raw(read)) };
    }
    let outcome = (|| {
        if write && !db_ptr.is_null() {
            let shared = unsafe { &*db_ptr };
            let mut db = shared
                .engine
                .lock()
                .map_err(|_| fail("execution", "database mutex poisoned"))?;
            db.rollback_transaction().map_err(|e| engine_error(&e))?;
        }
        Ok(())
    })();
    tx_release_db(tx);
    outcome
}

extern "C" fn tx_finalize(payload: *mut c_void) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let tx = payload.cast::<TransactionHandle>();
        let _ = tx_abandon(tx);
        tx_clear_error(tx);
    }));
}

fn tx_alloc(read_only: bool) -> *mut TransactionHandle {
    let tx = unsafe {
        moonbit_make_external_object(
            Some(tx_finalize),
            std::mem::size_of::<TransactionHandle>() as u32,
        )
    }
    .cast::<TransactionHandle>();
    if !tx.is_null() {
        unsafe {
            ptr::write(
                tx,
                TransactionHandle {
                    db: ptr::null_mut(),
                    read: ptr::null_mut(),
                    active: false,
                    read_only,
                    error: ptr::null_mut(),
                },
            );
        }
    }
    tx
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_tx_begin(
    parent: *mut Handle,
    read_only: i32,
) -> *mut TransactionHandle {
    let tx = tx_alloc(read_only != 0);
    if tx.is_null() {
        return tx;
    }
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if parent.is_null() || unsafe { (*parent).db }.is_null() {
            return Err(fail("closed", "database is closed"));
        }
        let shared = unsafe { &*(*parent).db }.clone();
        let mut locked = shared
            .engine
            .lock()
            .map_err(|_| fail("execution", "database mutex poisoned"))?;
        if shared.closed.load(Ordering::Acquire) {
            return Err(fail("closed", "database is closed"));
        }
        if read_only != 0 {
            let read = locked
                .begin_read_transaction()
                .map_err(|e| engine_error(&e))?;
            unsafe {
                (*tx).read = Box::into_raw(Box::new(read));
            }
        } else {
            locked.begin_transaction().map_err(|e| engine_error(&e))?;
        }
        unsafe {
            (*tx).db = Box::into_raw(Box::new(Arc::clone(&shared)));
            (*tx).active = true;
        }
        Ok(())
    }));
    match outcome {
        Ok(Ok(())) => (),
        Ok(Err(err)) => tx_set_error(tx, err),
        Err(_) => tx_set_error(tx, fail("panic", "panic during transaction begin")),
    }
    tx
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_tx_is_open(tx: *mut TransactionHandle) -> i32 {
    if tx.is_null() || !unsafe { (*tx).active } {
        return 0;
    }
    let db_ptr = unsafe { (*tx).db };
    if db_ptr.is_null() {
        return 0;
    }
    i32::from(!unsafe { &*db_ptr }.closed.load(Ordering::Acquire))
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_tx_error(tx: *mut TransactionHandle) -> *mut u8 {
    if tx.is_null() || unsafe { (*tx).error }.is_null() {
        return to_moonbit_bytes(error_json(fail("closed", "transaction is closed")).as_bytes());
    }
    to_moonbit_bytes(unsafe { &*(*tx).error }.as_bytes())
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_tx_query(
    tx: *mut TransactionHandle,
    statement: *const u8,
    statement_len: i32,
    params: *const u8,
    params_len: i32,
    is_sql: i32,
) -> *mut u8 {
    if tx.is_null() || !unsafe { (*tx).active } {
        return to_moonbit_bytes(&[]);
    }
    tx_clear_error(tx);
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let stmt = unsafe { input(statement, statement_len, "statement") }?;
        let args = unsafe { input(params, params_len, "params") }?;
        let db_ptr = unsafe { (*tx).db };
        if db_ptr.is_null() {
            return Err(fail("closed", "transaction is closed"));
        }
        let shared = unsafe { &*db_ptr };
        if shared.closed.load(Ordering::Acquire) {
            return Err(fail("closed", "database is closed"));
        }
        if unsafe { (*tx).read_only } {
            let read = unsafe { &mut *(*tx).read };
            let result = if is_sql != 0 {
                read.query_sql_with_params(stmt, &sql_params(args)?)
            } else {
                read.query_with_params(stmt, &named_params(args)?)
            }
            .map_err(|e| engine_error(&e))?;
            Ok::<String, Failure>(encode_result(&result))
        } else {
            let mut db = shared
                .engine
                .lock()
                .map_err(|_| fail("execution", "database mutex poisoned"))?;
            if shared.closed.load(Ordering::Acquire) {
                return Err(fail("closed", "database is closed"));
            }
            let result = if is_sql != 0 {
                db.transaction_query_sql_with_params(stmt, &sql_params(args)?)
            } else {
                db.transaction_query_with_params(stmt, &named_params(args)?)
            }
            .map_err(|e| engine_error(&e))?;
            Ok::<String, Failure>(encode_result(&result))
        }
    }));
    match outcome {
        Ok(Ok(json)) => to_moonbit_bytes(json.as_bytes()),
        Ok(Err(err)) => {
            tx_set_error(tx, err);
            to_moonbit_bytes(&[])
        }
        Err(_) => {
            tx_set_error(tx, fail("panic", "panic during transaction query"));
            to_moonbit_bytes(&[])
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_tx_finish(tx: *mut TransactionHandle, commit: i32) -> *mut u8 {
    if tx.is_null() || !unsafe { (*tx).active } {
        return to_moonbit_bytes(&[]);
    }
    tx_clear_error(tx);
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let db_ptr = unsafe { (*tx).db };
        if db_ptr.is_null() {
            return Err(fail("closed", "transaction is closed"));
        }
        let shared = unsafe { &*db_ptr };
        if shared.closed.load(Ordering::Acquire) {
            tx_abandon(tx)?;
            return Err(fail("closed", "database is closed"));
        }
        if unsafe { (*tx).read_only } {
            tx_abandon(tx)?;
            return Ok::<String, Failure>(r#"{"columns":[],"rows":[]}"#.to_owned());
        }
        if commit == 0 {
            tx_abandon(tx)?;
            return Ok(r#"{"columns":[],"rows":[]}"#.to_owned());
        }
        // Consume the transaction before calling commit: failed commit must
        // never leave a committable prefix or keep parent handles locked.
        unsafe {
            (*tx).active = false;
        }
        let mut db = shared
            .engine
            .lock()
            .map_err(|_| fail("execution", "database mutex poisoned"))?;
        if shared.closed.load(Ordering::Acquire) {
            db.rollback_transaction().map_err(|e| engine_error(&e))?;
            return Err(fail("closed", "database is closed"));
        }
        let result = db.commit_transaction().map_err(|e| engine_error(&e))?;
        Ok(encode_result(&result))
    }));
    if !unsafe { (*tx).active } {
        tx_release_db(tx);
    }
    match outcome {
        Ok(Ok(json)) => to_moonbit_bytes(json.as_bytes()),
        Ok(Err(err)) => {
            tx_set_error(tx, err);
            to_moonbit_bytes(&[])
        }
        Err(_) => {
            tx_set_error(tx, fail("panic", "panic during transaction finish"));
            to_moonbit_bytes(&[])
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_tx_close(tx: *mut TransactionHandle) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Err(err) = tx_abandon(tx) {
            tx_set_error(tx, err);
        }
    }));
}
