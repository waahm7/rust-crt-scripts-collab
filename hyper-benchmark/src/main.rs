use bytes::Bytes;
use clap::{Parser, ValueEnum};
use http_body_util::{BodyExt, Full};
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{Method, Request, Uri};
use hyper_tls::HttpsConnector;

use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use std::{
    iter::repeat_with,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{task::JoinSet, time::sleep};

#[derive(ValueEnum, Clone)]
enum TaskType {
    #[clap(name = "upload", help = "run upload benchmark")]
    Upload,
    #[clap(name = "download", help = "run download benchmark")]
    Download,
}

#[derive(Parser)]
#[command()]
struct Args {
    #[arg(help = "# parallel HTTP requests")]
    concurrency: usize,
    #[arg(help = "# seconds to run benchmark")]
    duration_secs: u64,
    #[arg(help = "presigned URL")]
    presigned_url: String,
    #[arg(value_enum, help = "Which benchmark to run? i.e upload or download")]
    action: TaskType,
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

type DownloadHttpClient = Client<
    HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    http_body_util::Empty<Bytes>,
>;
type UploadHttpClient = Client<
    HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    http_body_util::Full<Bytes>,
>;

struct Context {
    duration: Duration,
    url: Uri,
    download_client: DownloadHttpClient,
    upload_client: UploadHttpClient,
    is_running: AtomicBool,
    bytes_transferred: AtomicU64,
}

fn multipart_request_for_upload(body_length: usize, url: Uri) -> Request<Full<Bytes>> {
    let random_data_for_upload: Bytes = {
        let mut rng = fastrand::Rng::new();
        // TODO: take the size as input
        let data: Vec<u8> = repeat_with(|| rng.u8(..)).take(body_length).collect();
        data.into()
    };
    // Define the boundary for the multipart form data
    let boundary = "my_boundary";

    // Construct the multipart body manually
    let mut body = Vec::new();

    // Add the first part (text fields)
    body.extend(format!("--{}\r\n", boundary).as_bytes());
    body.extend(b"Content-Disposition: form-data; name=\"key\"\r\n\r\n");
    body.extend(b"test-rust-upload.txt\r\n");

    body.extend(format!("--{}\r\n", boundary).as_bytes());
    body.extend(b"Content-Disposition: form-data; name=\"AWSAccessKeyId\"\r\n\r\n");
    body.extend(b"\r\n");

    body.extend(format!("--{}\r\n", boundary).as_bytes());
    body.extend(b"Content-Disposition: form-data; name=\"policy\"\r\n\r\n");
    body.extend(b"\r\n");

    body.extend(format!("--{}\r\n", boundary).as_bytes());
    body.extend(b"Content-Disposition: form-data; name=\"signature\"\r\n\r\n");
    body.extend(b"\r\n");

    // Add the file part
    body.extend(format!("--{}\r\n", boundary).as_bytes());
    body.extend(
        b"Content-Disposition: form-data; name=\"file\"; filename=\"test-rust-upload.txt\"\r\n",
    );
    body.extend(b"Content-Type: application/octet-stream\r\n\r\n");

    body.extend(&random_data_for_upload.clone());
    body.extend(b"\r\n");

    // End the multipart form data
    body.extend(format!("--{}--\r\n", boundary).as_bytes());

    let bytes: Bytes = body.into();
    Request::builder()
        .method(Method::POST)
        .uri(url) // Set your URI here
        .header(
            CONTENT_TYPE,
            format!("multipart/form-data; boundary={}", boundary),
        )
        .header(CONTENT_LENGTH, bytes.len().to_string())
        .body(bytes.clone().into())
        .expect("request successful")
}

async fn async_main(args: Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let download_client = Client::builder(TokioExecutor::new())
        .build::<_, http_body_util::Empty<Bytes>>(HttpsConnector::new());

    let upload_client = Client::builder(TokioExecutor::new())
        .build::<_, http_body_util::Full<Bytes>>(HttpsConnector::new());

    let ctx = Arc::new(Context {
        duration: Duration::from_secs(args.duration_secs),
        url: args.presigned_url.parse()?,
        download_client,
        upload_client,
        is_running: AtomicBool::new(true),
        bytes_transferred: AtomicU64::new(0),
    });

    let mut tasks = JoinSet::new();
    for _ in 0..args.concurrency {
        match args.action {
            TaskType::Upload => tasks.spawn(upload_task(
                ctx.clone(),
                multipart_request_for_upload(8 * 1024 * 1024, ctx.url.clone()),
            )),
            TaskType::Download => tasks.spawn(download_task(ctx.clone())),
        };
    }

    tasks.spawn(timekeeper_task(ctx.clone()));

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}

async fn upload_task(
    ctx: Arc<Context>,
    request: Request<Full<Bytes>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    while ctx.is_running.load(Ordering::SeqCst) {
        let response = ctx.upload_client.request(request.clone()).await?;
        //println!("{:?}", response);
        if response.status() == 200 || response.status() == 204 {
            ctx.bytes_transferred
                .fetch_add(8 * 1024 * 1024 as u64, Ordering::SeqCst);
        }
    }
    Ok(())
}
// Keep doing HTTP requests until ctx.is_running becomes false.
async fn download_task(ctx: Arc<Context>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    while ctx.is_running.load(Ordering::SeqCst) {
        let mut response = ctx.download_client.get(ctx.url.clone()).await?;
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
