import asyncio
import os
import json
import uuid
import string
import random
from aiohttp import web, WSMsgType


class TunnelServer:
    def __init__(self, public_port=9000, base_domain="localhost"):
        self.public_port = public_port
        self.base_domain = base_domain
        self.tunnels = {}
        self.pending_requests = {}

    def generate_id(self):
        chars = string.ascii_lowercase + string.digits
        while True:
            tid = ''.join(random.choices(chars, k=6))
            if tid not in self.tunnels:
                return tid

    async def handle_client_ws(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        tunnel_id = self.generate_id()
        self.tunnels[tunnel_id] = ws

        public_url = f"https://{self.base_domain}/{tunnel_id}"
        await ws.send_json({"type": "registered", "tunnel_id": tunnel_id, "url": public_url})
        print(f"[+] Tunnel {tunnel_id} → {public_url}")

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    req_id = data.get("req_id")
                    if req_id in self.pending_requests:
                        self.pending_requests[req_id].set_result(data)
                elif msg.type == WSMsgType.ERROR:
                    break
        finally:
            del self.tunnels[tunnel_id]
            print(f"[-] Tunnel {tunnel_id} disconnected")
        return ws

    async def handle_proxy(self, request):
        tunnel_id = request.match_info["tunnel_id"]
        if tunnel_id not in self.tunnels:
            return web.Response(status=502, text="Tunnel not connected")

        ws = self.tunnels[tunnel_id]
        req_id = str(uuid.uuid4())
        path = "/" + request.match_info.get("path", "")
        body = await request.read()

        future = asyncio.get_event_loop().create_future()
        self.pending_requests[req_id] = future

        await ws.send_json({
            "type": "request",
            "req_id": req_id,
            "method": request.method,
            "path": path,
            "headers": dict(request.headers),
            "body": body.decode("utf-8", errors="replace"),
        })

        try:
            response_data = await asyncio.wait_for(future, timeout=30)
        except asyncio.TimeoutError:
            return web.Response(status=504, text="Tunnel timeout")
        finally:
            self.pending_requests.pop(req_id, None)

        return web.Response(
            status=response_data.get("status", 200),
            body=response_data.get("body", ""),
            headers=response_data.get("headers", {}),
        )

    async def handle_list(self, request):
        tunnels = []
        for tid in self.tunnels:
            tunnels.append({"tunnel_id": tid, "url": f"https://{self.base_domain}/{tid}"})
        return web.json_response(tunnels)

    async def handle_health(self, request):
        return web.json_response({"status": "ok", "tunnels": len(self.tunnels)})

    def run(self):
        app = web.Application()
        app.router.add_get("/ws", self.handle_client_ws)
        app.router.add_get("/api/tunnels", self.handle_list)
        app.router.add_get("/api/health", self.handle_health)
        app.router.add_route("*", "/{tunnel_id}/{path:.*}", self.handle_proxy)
        print(f"Tunnel server on port {self.public_port} | domain: {self.base_domain}")
        web.run_app(app, port=self.public_port)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Tunnel relay server")
    parser.add_argument("-p", "--port", type=int, default=int(os.environ.get("PORT", 9000)))
    parser.add_argument("-d", "--domain", default=os.environ.get("TUNNEL_DOMAIN", "localhost"))
    args = parser.parse_args()
    TunnelServer(public_port=args.port, base_domain=args.domain).run()
