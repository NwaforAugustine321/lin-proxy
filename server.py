import asyncio
import sys
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
        self.subdomain_map = {}
        self.pending_requests = {}

    def generate_subdomain(self):
        chars = string.ascii_lowercase + string.digits
        while True:
            sub = ''.join(random.choices(chars, k=6))
            if sub not in self.subdomain_map:
                return sub

    async def handle_client_ws(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        tunnel_id = str(uuid.uuid4())[:8]
        subdomain = self.generate_subdomain()
        self.tunnels[tunnel_id] = {"ws": ws, "subdomain": subdomain}
        self.subdomain_map[subdomain] = tunnel_id

        public_url = f"https://{subdomain}.{self.base_domain}"
        await ws.send_json({"type": "registered", "tunnel_id": tunnel_id, "subdomain": subdomain, "url": public_url})
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
            self.subdomain_map.pop(subdomain, None)
            del self.tunnels[tunnel_id]
            print(f"[-] Tunnel {tunnel_id} disconnected")
        return ws

    def resolve_tunnel(self, request):
        host = request.headers.get("Host", "")
        subdomain = host.split(".")[0] if "." in host else None
        if subdomain and subdomain in self.subdomain_map:
            return self.subdomain_map[subdomain]

        tunnel_id = request.match_info.get("tunnel_id")
        if tunnel_id and tunnel_id in self.tunnels:
            return tunnel_id
        return None

    async def handle_subdomain_request(self, request):
        host = request.headers.get("Host", "")
        subdomain = host.split(".")[0] if "." in host else None
        if not subdomain or subdomain not in self.subdomain_map:
            return web.Response(status=404, text="Tunnel not found")
        tunnel_id = self.subdomain_map[subdomain]
        return await self._proxy(request, tunnel_id, request.path)

    async def handle_path_request(self, request):
        tunnel_id = request.match_info["tunnel_id"]
        if tunnel_id not in self.tunnels:
            return web.Response(status=502, text="Tunnel not connected")
        path = "/" + request.match_info.get("path", "")
        return await self._proxy(request, tunnel_id, path)

    async def _proxy(self, request, tunnel_id, path):
        ws = self.tunnels[tunnel_id]["ws"]
        req_id = str(uuid.uuid4())
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
        for tid, info in self.tunnels.items():
            tunnels.append({"tunnel_id": tid, "subdomain": info["subdomain"], "url": f"https://{info['subdomain']}.{self.base_domain}"})
        return web.json_response(tunnels)

    def run(self):
        app = web.Application()
        app.router.add_get("/ws", self.handle_client_ws)
        app.router.add_get("/api/tunnels", self.handle_list)
        app.router.add_route("*", "/t/{tunnel_id}/{path:.*}", self.handle_path_request)
        app.router.add_route("*", "/{path:.*}", self.handle_subdomain_request)
        print(f"Tunnel server on port {self.public_port} | domain: *.{self.base_domain}")
        web.run_app(app, port=self.public_port)


if __name__ == "__main__":
    import argparse
    import os
    parser = argparse.ArgumentParser(description="Tunnel relay server")
    parser.add_argument("-p", "--port", type=int, default=int(os.environ.get("PORT", 9000)))
    parser.add_argument("-d", "--domain", default=os.environ.get("TUNNEL_DOMAIN", "localhost"))
    args = parser.parse_args()
    TunnelServer(public_port=args.port, base_domain=args.domain).run()
