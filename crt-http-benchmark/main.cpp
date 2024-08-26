#include <atomic>
#include <chrono>
#include <iostream>
#include <semaphore>
#include <thread>

#include <aws/common/uri.h>
#include <aws/http/connection_manager.h>
#include <aws/http/request_response.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/io/socket.h>
#include <aws/io/tls_channel_handler.h>

using namespace std;
using namespace std::chrono;

// exit due to failure
[[noreturn]] void fail(string_view msg)
{
    cerr << "FAIL - " << msg << endl;
    _Exit(255);
}

class BenchmarkRunner
{
  public:
    // CLI Args
    int durationSecs;
    aws_uri uri;

    // CRT boilerplate
    aws_allocator *alloc;
    aws_logger logger;
    aws_event_loop_group *eventLoopGroup;
    aws_host_resolver *hostResolver;
    aws_client_bootstrap *clientBootstrap;
    aws_tls_ctx *tlsCtx;
    aws_http_connection_manager *connectionManager;
    aws_http_message *requestMsg;

    // Running state
    atomic<bool> isRunning;
    atomic<uint64_t> bytesTransferred;
    counting_semaphore<INT_MAX> concurrencySemaphore;

    BenchmarkRunner(int concurrency, int durationSecs, const char *uri_cstr) : concurrencySemaphore(concurrency)
    {
        this->durationSecs = durationSecs;
        this->alloc = aws_default_allocator();

        aws_http_library_init(alloc);

        struct aws_logger_standard_options logOpts;
        AWS_ZERO_STRUCT(logOpts);
        logOpts.level = AWS_LL_ERROR;
        logOpts.file = stderr;
        AWS_FATAL_ASSERT(aws_logger_init_standard(&this->logger, alloc, &logOpts) == 0);
        aws_logger_set(&this->logger);

        auto uri_cursor = aws_byte_cursor_from_c_str(uri_cstr);
        if (aws_uri_init_parse(&this->uri, alloc, &uri_cursor) != 0)
        {
            fail(string("invalid URI: ") + uri_cstr);
        }

        this->eventLoopGroup = aws_event_loop_group_new_default(alloc, 0 /*max-threads*/, NULL /*shutdown-options*/);
        AWS_FATAL_ASSERT(eventLoopGroup != NULL);

        aws_host_resolver_default_options resolverOpts;
        AWS_ZERO_STRUCT(resolverOpts);
        resolverOpts.max_entries = 8;
        resolverOpts.el_group = eventLoopGroup;
        this->hostResolver = aws_host_resolver_new_default(alloc, &resolverOpts);
        AWS_FATAL_ASSERT(this->hostResolver != NULL);

        aws_client_bootstrap_options bootstrapOpts;
        AWS_ZERO_STRUCT(bootstrapOpts);
        bootstrapOpts.event_loop_group = this->eventLoopGroup;
        bootstrapOpts.host_resolver = this->hostResolver;
        this->clientBootstrap = aws_client_bootstrap_new(alloc, &bootstrapOpts);
        AWS_FATAL_ASSERT(this->clientBootstrap != NULL);

        aws_tls_ctx_options tlsCtxOpts;
        aws_tls_ctx_options_init_default_client(&tlsCtxOpts, alloc);
        this->tlsCtx = aws_tls_client_ctx_new(alloc, &tlsCtxOpts);
        AWS_FATAL_ASSERT(this->tlsCtx != NULL);

        aws_tls_connection_options tlsConnOpts;
        aws_tls_connection_options_init_from_ctx(&tlsConnOpts, tlsCtx);

        aws_socket_options sockOpts;
        AWS_ZERO_STRUCT(sockOpts);
        sockOpts.connect_timeout_ms = 10000;

        uint32_t port = aws_uri_port(&this->uri);
        if (port == 0)
            port = aws_byte_cursor_eq_c_str_ignore_case(aws_uri_scheme(&this->uri), "https") ? 443 : 80;

        aws_http_connection_manager_options connMgrOpts;
        AWS_ZERO_STRUCT(connMgrOpts);
        connMgrOpts.bootstrap = this->clientBootstrap;
        connMgrOpts.socket_options = &sockOpts;
        connMgrOpts.tls_connection_options = &tlsConnOpts;
        connMgrOpts.host = *aws_uri_host_name(&this->uri);
        connMgrOpts.port = port;
        connMgrOpts.max_connections = static_cast<size_t>(concurrency);
        this->connectionManager = aws_http_connection_manager_new(alloc, &connMgrOpts);
        AWS_FATAL_ASSERT(this->connectionManager != NULL);

        this->requestMsg = aws_http_message_new_request(alloc);
        AWS_FATAL_ASSERT(this->requestMsg);
        aws_http_message_set_request_method(this->requestMsg, aws_byte_cursor_from_c_str("GET"));
        aws_http_message_set_request_path(this->requestMsg, *aws_uri_path_and_query(&this->uri));
        aws_http_message_add_header(
            this->requestMsg, aws_http_header{aws_byte_cursor_from_c_str("Host"), *aws_uri_host_name(&this->uri)});
    }

    ~BenchmarkRunner()
    {
        aws_http_message_release(this->requestMsg);
        aws_http_connection_manager_release(this->connectionManager);
        aws_tls_ctx_release(this->tlsCtx);
        aws_client_bootstrap_release(this->clientBootstrap);
        aws_host_resolver_release(this->hostResolver);
        aws_event_loop_group_release(this->eventLoopGroup);
        aws_http_library_clean_up();
        aws_logger_clean_up(&this->logger);
    }

    void run()
    {
        this->isRunning.store(true);
        auto workSubmissionThread = thread(workSubmissionThreadFn, this);

        // throw out any bytes transferred before we get into the core loop
        this->bytesTransferred.store(0);
        auto prevTime = high_resolution_clock::now();
        for (int sec = 0; sec < this->durationSecs; ++sec)
        {
            this_thread::sleep_for(1s);
            auto curTime = high_resolution_clock::now();
            duration<double> elapsedSecs = curTime - prevTime;
            prevTime = curTime;

            uint64_t bytes = this->bytesTransferred.exchange(0);
            uint64_t bits = bytes * 8;
            double gigabits = bits / 1000000000.0;
            double gigabitsPerSec = gigabits / elapsedSecs.count();
            printf("Secs:%d Gb/s:%f\n", sec + 1, gigabitsPerSec);
        }
        this->isRunning.store(false);

        workSubmissionThread.join();
    }

    static void workSubmissionThreadFn(BenchmarkRunner *runner);
};

class RequestTask
{
  public:
    BenchmarkRunner *runner;
    aws_http_connection *connection = NULL;
    aws_http_stream *stream = NULL;

    RequestTask(BenchmarkRunner *runner) : runner(runner) {}

    void run()
    {
        aws_http_connection_manager_acquire_connection(
            runner->connectionManager, &RequestTask::onConnectionAcquired, this);
    }

    ~RequestTask()
    {
        aws_http_stream_release(this->stream);
        aws_http_connection_manager_release_connection(this->runner->connectionManager, this->connection);
        this->runner->concurrencySemaphore.release();
    }

  private:
    static void onConnectionAcquired(struct aws_http_connection *connection, int errorCode, void *userData)
    {
        auto task = static_cast<RequestTask *>(userData);
        if (errorCode != 0)
        {
            printf("Failed to acquire connection: %s\n", aws_error_name(errorCode));
            delete task;
            return;
        }

        task->connection = connection;

        aws_http_make_request_options requestOpts;
        AWS_ZERO_STRUCT(requestOpts);
        requestOpts.self_size = sizeof(requestOpts);
        requestOpts.request = task->runner->requestMsg;
        requestOpts.on_complete = &onRequestComplete;
        requestOpts.on_response_body = &onIncomingBody;
        requestOpts.user_data = task;

        task->stream = aws_http_connection_make_request(task->connection, &requestOpts);
        if (task->stream == NULL)
        {
            printf("Failed to make request: %s\n", aws_error_name(aws_last_error()));
            delete task;
        }
        if (aws_http_stream_activate(task->stream) != 0)
        {
            printf("Failed to activate request: %s\n", aws_error_name(aws_last_error()));
            delete task;
        }
    }

    static void onRequestComplete(struct aws_http_stream *stream, int errorCode, void *userData)
    {
        auto task = static_cast<RequestTask *>(userData);
        if (errorCode != 0)
        {
            printf("HTTP Request Failed: %s\n", aws_error_name(errorCode));
        }
        else
        {
            int statusCode = 0;
            aws_http_stream_get_incoming_response_status(stream, &statusCode);
            if (statusCode < 200 || statusCode >= 300)
            {
                printf("HTTP Status: %d %s\n", statusCode, aws_http_status_text(statusCode));
            }
        }

        delete task;
    }

    static int onIncomingBody(struct aws_http_stream *stream, const struct aws_byte_cursor *data, void *userData)
    {
        auto task = static_cast<RequestTask *>(userData);
        task->runner->bytesTransferred.fetch_add(data->len);
        return AWS_OP_SUCCESS;
    }
};

void BenchmarkRunner::workSubmissionThreadFn(BenchmarkRunner *runner)
{
    while (true)
    {
        runner->concurrencySemaphore.acquire();

        // exit thread if benchmark is no longer running
        if (runner->isRunning.load() == false)
            return;

        auto requestTask = new RequestTask(runner);
        requestTask->run();
    }
}

int main(int argc, const char **argv)
{
    if (argc != 4)
    {
        fail(string("usage: ") + argv[0] + " CONCURRENCY DURATION_SECS URL");
    }
    int concurrency = stoi(argv[1]);
    int durationSecs = stoi(argv[2]);
    const char *uri = argv[3];

    auto runner = BenchmarkRunner(concurrency, durationSecs, uri);
    runner.run();
}
