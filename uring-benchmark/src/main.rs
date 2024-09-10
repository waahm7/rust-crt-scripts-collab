use std::{
    cmp::min,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use clap::Parser;
use http::Uri;
use http_body_util::BodyExt;
use hyper_tls::HttpsConnector;
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use tokio::task::JoinSet;

#[derive(Parser)]
#[command()]
struct Args {
    #[arg(help = "# parallel HTTP requests")]
    concurrency: usize,
    #[arg(help = "# seconds to run benchmark")]
    duration_secs: u64,
    #[arg(help = "presigned URL")]
    presigned_url: String,
}

type HttpClient = Client<
    HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    http_body_util::Empty<Bytes>,
>;

struct Context {
    duration: Duration,
    url: Uri,
    client: HttpClient,
    is_running: AtomicBool,
    bytes_transferred: AtomicU64,
}

type MyResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn main() -> MyResult {
    let args = Args::parse();

    let https = HttpsConnector::new();
    let client =
        Client::builder(TokioExecutor::new()).build::<_, http_body_util::Empty<Bytes>>(https);

    let ctx = Arc::new(Context {
        duration: Duration::from_secs(args.duration_secs),
        url: args.presigned_url.parse()?,
        client,
        is_running: AtomicBool::new(true),
        bytes_transferred: AtomicU64::new(0),
    });

    // tokio_uring doesn't come with a multi-threaded runtime.
    // When running tokio_uring::start(...) on a single thread, throughput peaked at 9Gb/s.
    // Therefore, throw together our own primitive multi-threaded runtime
    // by spinning up some number of threads with a tokio_uring runtime on each.
    // Distribute worker_tasks among those threads.
    let num_threads = min(args.concurrency, 48); //num_cpus::get() / 2);
    let mut total_concurrency = 0;
    let mut thread_handles: Vec<std::thread::JoinHandle<MyResult>> = vec![];
    for _ in 0..num_threads {
        let thread_concurrency = min(
            args.concurrency / num_threads,
            args.concurrency - total_concurrency,
        );
        total_concurrency += thread_concurrency;
        let ctx = ctx.clone();

        let handle =
            std::thread::spawn(move || tokio_uring::start(uring_thread(ctx, thread_concurrency)));

        thread_handles.push(handle);
    }

    // run timekeeper task on the main thread
    timekeeper_task(ctx);

    // wait for threads to join
    for handle in thread_handles {
        let _ = handle.join().unwrap();
    }

    Ok(())
}

// Each thread runs some number of worker_tasks concurrently
async fn uring_thread(ctx: Arc<Context>, thread_concurrency: usize) -> MyResult {
    let mut tasks = JoinSet::new();
    for _ in 0..thread_concurrency {
        tasks.spawn(worker_task(ctx.clone()));
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}

// Keep doing HTTP requests until ctx.is_running becomes false.
async fn worker_task(ctx: Arc<Context>) -> MyResult {
    while ctx.is_running.load(Ordering::SeqCst) {
        let mut response = ctx.client.get(ctx.url.clone()).await?;
        while let Some(frame_result) = response.body_mut().frame().await {
            let frame = frame_result?;
            if let Some(bytes) = frame.data_ref() {
                ctx.bytes_transferred
                    .fetch_add(bytes.len() as u64, Ordering::SeqCst);
            }
        }
    }

    Ok(())
}

// Once per second, print the throughput.
// Also, tell all workers to stop when enough time has passed.
fn timekeeper_task(ctx: Arc<Context>) {
    // throw out any bytes transferred before we get into the core loop
    ctx.bytes_transferred.store(0, Ordering::SeqCst);
    let mut prev_time = Instant::now();

    for secs in 0..ctx.duration.as_secs() {
        std::thread::sleep(Duration::from_secs(1));

        // get exactly how much time elapsed
        let cur_time = Instant::now();
        let elapsed = cur_time - prev_time;
        prev_time = cur_time;

        // how many bytes were transferred in that time?
        let bytes = ctx.bytes_transferred.swap(0, Ordering::SeqCst);
        let bits = bytes * 8;
        let gigabits = (bits as f64) / 1_000_000_000.0;
        let gigabits_per_sec = gigabits / elapsed.as_secs_f64();
        println!("Secs:{} Gb/s:{:.6}", secs + 1, gigabits_per_sec);
    }

    // tell workers to stop
    ctx.is_running.store(false, Ordering::SeqCst);
}
