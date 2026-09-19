#![allow(dead_code)]

use std::{
    future::Future,
    sync::{Mutex, MutexGuard},
    time::Duration,
};

pub async fn watchdog<F: Future>(future: F) -> F::Output {
    // Miri interprets far slower than native execution.
    let seconds = if cfg!(miri) { 600 } else { 2 };
    tokio::time::timeout(Duration::from_secs(seconds), future)
        .await
        .expect("runtime operation exceeded the deadlock watchdog")
}

pub fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
