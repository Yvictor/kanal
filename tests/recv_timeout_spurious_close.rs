//! Reproduces spurious `ReceiveErrorTimeout::Closed` from `recv_timeout()`
//! when the channel is still alive (sender not dropped, recv_count > 0).
//!
//! Scenario: unbounded channel, sender fires bursts of messages with gaps,
//! receiver loops with `recv_timeout()`. After processing the initial burst,
//! `recv_timeout` returns `Closed` on the next call even though both sender
//! and receiver are alive.
//!
//! Discovered in rshioaji where a C callback sends events to a kanal channel
//! and a Rust handler thread loops with `recv_timeout(100ms)`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn recv_timeout_should_not_return_closed_while_channel_alive() {
    let (sender, receiver) = kanal::unbounded::<String>();

    let received = Arc::new(AtomicUsize::new(0));
    let spurious_closed = Arc::new(AtomicUsize::new(0));
    let received_clone = received.clone();
    let spurious_clone = spurious_closed.clone();

    // Receiver: loops with recv_timeout like a background event handler
    let recv_handle = std::thread::spawn(move || {
        loop {
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(msg) => {
                    received_clone.fetch_add(1, Ordering::SeqCst);
                    if msg == "done" {
                        break;
                    }
                }
                Err(kanal::ReceiveErrorTimeout::Timeout) => continue,
                Err(kanal::ReceiveErrorTimeout::SendClosed) => break,
                Err(kanal::ReceiveErrorTimeout::Closed) => {
                    // Channel still alive — this is the bug
                    spurious_clone.fetch_add(1, Ordering::SeqCst);
                    continue; // workaround: retry
                }
            }
        }
    });

    // Sender: burst of 2 messages, gap, then more messages
    let send_handle = std::thread::spawn(move || {
        sender.send("event_1".to_string()).unwrap();
        sender.send("event_2".to_string()).unwrap();

        // Gap — receiver will hit recv_timeout multiple times here
        std::thread::sleep(Duration::from_millis(500));

        sender.send("event_3".to_string()).unwrap();
        std::thread::sleep(Duration::from_millis(200));
        sender.send("done".to_string()).unwrap();
    });

    send_handle.join().unwrap();
    recv_handle.join().unwrap();

    let total_received = received.load(Ordering::SeqCst);
    let total_spurious = spurious_closed.load(Ordering::SeqCst);

    assert_eq!(
        total_received, 4,
        "Expected 4 messages, got {}. Spurious Closed: {}",
        total_received, total_spurious
    );
    assert_eq!(
        total_spurious, 0,
        "recv_timeout returned Closed {} times while channel was still alive",
        total_spurious
    );
}

/// Same test but with clone_sync() pattern (async receiver → sync clone)
#[test]
fn recv_timeout_clone_sync_should_not_return_closed() {
    let (sender, receiver) = kanal::unbounded::<String>();

    let async_rx = receiver.clone_async();
    let sync_rx = async_rx.clone_sync();
    drop(async_rx);
    drop(receiver);

    let received = Arc::new(AtomicUsize::new(0));
    let spurious_closed = Arc::new(AtomicUsize::new(0));
    let received_clone = received.clone();
    let spurious_clone = spurious_closed.clone();

    let recv_handle = std::thread::spawn(move || {
        loop {
            match sync_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(msg) => {
                    received_clone.fetch_add(1, Ordering::SeqCst);
                    if msg == "done" {
                        break;
                    }
                }
                Err(kanal::ReceiveErrorTimeout::Timeout) => continue,
                Err(kanal::ReceiveErrorTimeout::SendClosed) => break,
                Err(kanal::ReceiveErrorTimeout::Closed) => {
                    spurious_clone.fetch_add(1, Ordering::SeqCst);
                    continue;
                }
            }
        }
    });

    let send_handle = std::thread::spawn(move || {
        sender.send("event_1".to_string()).unwrap();
        sender.send("event_2".to_string()).unwrap();
        std::thread::sleep(Duration::from_millis(500));
        sender.send("event_3".to_string()).unwrap();
        std::thread::sleep(Duration::from_millis(200));
        sender.send("done".to_string()).unwrap();
    });

    send_handle.join().unwrap();
    recv_handle.join().unwrap();

    let total_received = received.load(Ordering::SeqCst);
    let total_spurious = spurious_closed.load(Ordering::SeqCst);

    assert_eq!(total_received, 4, "Expected 4, got {}", total_received);
    assert_eq!(total_spurious, 0, "Got {} spurious Closed", total_spurious);
}

/// Stress test: 100 iterations with varied timing
#[test]
fn recv_timeout_stress_no_spurious_closed() {
    for iteration in 0..100 {
        let (sender, receiver) = kanal::unbounded::<u32>();

        let spurious = Arc::new(AtomicUsize::new(0));
        let spurious_clone = spurious.clone();

        let recv_handle = std::thread::spawn(move || {
            loop {
                match receiver.recv_timeout(Duration::from_millis(10)) {
                    Ok(v) if v == u32::MAX => break,
                    Ok(_) => {}
                    Err(kanal::ReceiveErrorTimeout::Timeout) => continue,
                    Err(kanal::ReceiveErrorTimeout::SendClosed) => break,
                    Err(kanal::ReceiveErrorTimeout::Closed) => {
                        spurious_clone.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }
                }
            }
        });

        let send_handle = std::thread::spawn(move || {
            for i in 0u32..10 {
                sender.send(i).unwrap();
                if i % 3 == 0 {
                    std::thread::sleep(Duration::from_millis(15));
                }
            }
            sender.send(u32::MAX).unwrap();
        });

        send_handle.join().unwrap();
        recv_handle.join().unwrap();

        let s = spurious.load(Ordering::SeqCst);
        assert_eq!(s, 0, "Iteration {}: {} spurious Closed errors", iteration, s);
    }
}
