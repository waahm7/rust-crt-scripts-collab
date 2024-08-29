from aiohttp import web

async def handle_put(request):
    body = await request.content.read()  # Read the entire request body
    print(f"Received PUT request with body: {len(body)}")  
    return web.Response(status=200)

async def init_app():
    app = web.Application()
    app.router.add_route('PUT', '/', handle_put)
    return app

def run_server():
    app = init_app()

    # Tuning settings
    web.run_app(
        app,
        port=8080,
        handle_signals=True,
        access_log=None,  # Disable access log for performance
        keepalive_timeout=5,  # Adjust keepalive timeout
        shutdown_timeout=60,  # Time to wait before shutdown
        reuse_address=True,  # Reuse socket address
        reuse_port=True,  # Allow multiple workers to share the same port
    )

if __name__ == '__main__':
    run_server()
