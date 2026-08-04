//! A controllable TCP proxy in front of the live cppagent, for the KUBERNETES leg.
//!
//! HARNESS, not product code. The pod is pointed at `http://192.168.1.224:5998` instead of the
//! agent's own `:5010`; this process forwards 5998 -> 127.0.0.1:5010. Because the break is made
//! HERE, the shared `mtc-e2e-agent` container is never paused, stopped or otherwise disturbed —
//! and neither is the GREENGRASS lab leg's SHDR feeder that keeps it live.
//!
//! Control file (argv[2], one command per line, appended from the shell):
//!   on     forward normally
//!   hang   stop moving bytes in BOTH directions while keeping the sockets open — the agent goes
//!          silent from the pod's point of view, which is exactly what the passive-quality
//!          liveness clock judges (stale -> expired)
//!   off    drop every open connection and refuse new ones — a real link loss
//!   quit   exit

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

const ON: u8 = 0;
const HANG: u8 = 1;
const OFF: u8 = 2;

static MODE: AtomicU8 = AtomicU8::new(ON);

fn wall() -> String {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = d.as_secs();
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        (secs / 3600) % 24,
        (secs / 60) % 60,
        secs % 60,
        d.subsec_millis()
    )
}

async fn pump<R, W>(mut r: R, mut w: W, mut mode: watch::Receiver<u8>)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = vec![0u8; 32 * 1024];
    loop {
        // Gate: while HANG, move nothing (TCP backpressure does the rest). While OFF, die.
        loop {
            let m = *mode.borrow_and_update();
            if m == OFF {
                return;
            }
            if m == ON {
                break;
            }
            if mode.changed().await.is_err() {
                return;
            }
        }
        tokio::select! {
            r2 = mode.changed() => {
                if r2.is_err() { return; }
            }
            n = r.read(&mut buf) => match n {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    if w.write_all(&buf[..n]).await.is_err() { return; }
                    let _ = w.flush().await;
                }
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let listen: u16 = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "5998".into())
        .parse()
        .expect("listen port");
    let control = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "proxy-control.txt".into());
    let target = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "127.0.0.1:5010".into());

    let (tx, rx) = watch::channel(ON);
    let listener = TcpListener::bind(("0.0.0.0", listen))
        .await
        .unwrap_or_else(|e| panic!("bind {listen}: {e}"));
    println!("[{}] proxy 0.0.0.0:{listen} -> {target}", wall());
    println!("[{}] control file: {control}", wall());

    let accept_rx = rx.clone();
    let target2 = target.clone();
    tokio::spawn(async move {
        loop {
            let Ok((inbound, peer)) = listener.accept().await else {
                return;
            };
            if MODE.load(Ordering::Relaxed) == OFF {
                println!("[{}] refused {peer} (mode=off)", wall());
                drop(inbound);
                continue;
            }
            let rx = accept_rx.clone();
            let target = target2.clone();
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect(&target).await else {
                    return;
                };
                let _ = inbound.set_nodelay(true);
                let _ = outbound.set_nodelay(true);
                let (ri, wi) = inbound.into_split();
                let (ro, wo) = outbound.into_split();
                let a = tokio::spawn(pump(ri, wo, rx.clone()));
                let b = tokio::spawn(pump(ro, wi, rx));
                let _ = a.await;
                b.abort();
            });
        }
    });

    let mut consumed = 0usize;
    loop {
        let text = tokio::fs::read_to_string(&control).await.unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        if lines.len() > consumed {
            for line in &lines[consumed..] {
                match line.trim() {
                    "" => {}
                    "on" => {
                        MODE.store(ON, Ordering::Relaxed);
                        let _ = tx.send(ON);
                        println!("[{}] PROXY MODE = on (forwarding)", wall());
                    }
                    "hang" => {
                        MODE.store(HANG, Ordering::Relaxed);
                        let _ = tx.send(HANG);
                        println!("[{}] PROXY MODE = hang (sockets open, no bytes)", wall());
                    }
                    "off" => {
                        MODE.store(OFF, Ordering::Relaxed);
                        let _ = tx.send(OFF);
                        println!("[{}] PROXY MODE = off (connections dropped, refusing)", wall());
                    }
                    "quit" => {
                        println!("[{}] quit", wall());
                        return;
                    }
                    other => println!("[{}] unknown proxy command `{other}`", wall()),
                }
            }
            consumed = lines.len();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
