//! EXPERIMENT ONLY: a std-Mutex-API-compatible cell with NO atomic
//! instructions, to measure the lock share of h2 termination cost on a
//! single-threaded runtime. UNSOUND if two threads actually touch it — the
//! benches drive everything on one current_thread runtime. A non-atomic
//! borrow flag catches accidental re-entrancy in debug fashion.
#![allow(missing_docs)]

use std::cell::{Cell, UnsafeCell};
use std::ops::{Deref, DerefMut};

pub struct Mutex<T> {
    busy: Cell<bool>,
    val: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for Mutex<T> {}
unsafe impl<T: Send> Sync for Mutex<T> {}

pub struct MutexGuard<'a, T> {
    m: &'a Mutex<T>,
}

impl<T> Mutex<T> {
    pub fn new(val: T) -> Self {
        Mutex { busy: Cell::new(false), val: UnsafeCell::new(val) }
    }

    #[inline]
    pub fn lock(&self) -> Result<MutexGuard<'_, T>, ()> {
        if self.busy.replace(true) {
            panic!("cheap_mutex: re-entrant lock (would deadlock with std Mutex)");
        }
        Ok(MutexGuard { m: self })
    }
}

impl<T> Drop for MutexGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        self.m.busy.set(false);
    }
}

impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        unsafe { &*self.m.val.get() }
    }
}

impl<T> DerefMut for MutexGuard<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.m.val.get() }
    }
}

impl<T> Mutex<T> {
    #[inline]
    pub fn try_lock(&self) -> Result<MutexGuard<'_, T>, ()> {
        if self.busy.replace(true) {
            return Err(());
        }
        Ok(MutexGuard { m: self })
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.try_lock() {
            Ok(g) => f.debug_tuple("Mutex").field(&&*g).finish(),
            Err(_) => f.write_str("Mutex(<locked>)"),
        }
    }
}
