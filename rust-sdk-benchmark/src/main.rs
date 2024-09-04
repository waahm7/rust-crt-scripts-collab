use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use aws_config::Region;
use clap::Parser;
use tokio::{task::JoinSet, time::sleep};

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

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        // .worker_threads(64) // default is # cores
        .build()
        .unwrap();

    runtime.block_on(async_main(args))?;
    Ok(())
}

struct Context {
    duration: Duration,
    bucket: String,
    key: String,
    client: aws_sdk_s3::Client,
    is_running: AtomicBool,
    bytes_transferred: AtomicU64,
}

async fn async_main(args: Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let presigned_url: http::Uri = args.presigned_url.parse()?;
    // host looks like "bucket-name.s3.region-name.amazonaws.com"
    let host_parts: Vec<&str> = presigned_url.host().unwrap().split(".").collect();
    let bucket = host_parts[0].to_owned();
    let region = host_parts[2].to_owned();
    let key = presigned_url.path()[1..].to_owned();

    let sdk_config = aws_config::from_env()
        .region(Region::new(region))
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&sdk_config);

    let ctx = Arc::new(Context {
        duration: Duration::from_secs(args.duration_secs),
        bucket,
        key,
        client,
        is_running: AtomicBool::new(true),
        bytes_transferred: AtomicU64::new(0),
    });

    let mut tasks = JoinSet::new();
    for _ in 0..args.concurrency {
        tasks.spawn(worker_task(ctx.clone()));
    }

    tasks.spawn(timekeeper_task(ctx.clone()));

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}

// Keep doing HTTP requests until ctx.is_running becomes false.
async fn worker_task(ctx: Arc<Context>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    while ctx.is_running.load(Ordering::SeqCst) {
        let mut object = ctx
            .client
            .get_object()
            .bucket(ctx.bucket.clone())
            .key(ctx.key.clone())
            .send()
            .await?;
        while let Some(bytes) = object.body.try_next().await? {
            ctx.bytes_transferred
                .fetch_add(bytes.len() as u64, Ordering::SeqCst);
        }
    }

    Ok(())
}

// Once per second, print the throughput.
// Also, tell all workers to stop when enough time has passed.
async fn timekeeper_task(
    ctx: Arc<Context>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // throw out any bytes transferred before we get into the core loop
    ctx.bytes_transferred.store(0, Ordering::SeqCst);
    let mut prev_time = Instant::now();

    for secs in 0..ctx.duration.as_secs() {
        sleep(Duration::from_secs(1)).await;

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

    Ok(())
}
