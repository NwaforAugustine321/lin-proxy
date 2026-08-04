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
        self.ws_bridges = {}

    def generate_id(self):
        chars = string.ascii_lowercase + string.digits
        while True:
            tid = ''.join(random.choices(chars, k=6))
            if tid not in self.tunnels:
                return tid

    async def handle_client_ws(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        requested_id = request.query.get("id")
        if requested_id and requested_id in self.tunnels:
            tunnel_id = requested_id
        elif requested_id and requested_id not in self.tunnels:
            tunnel_id = requested_id
        else:
            tunnel_id = self.generate_id()

        self.tunnels[tunnel_id] = ws

        public_url = f"https://{self.base_domain}/{tunnel_id}"
        await ws.send_json({"type": "registered", "tunnel_id": tunnel_id, "url": public_url})
        print(f"[+] Tunnel {tunnel_id} → {public_url}")

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    msg_type = data.get("type")
                    req_id = data.get("req_id")

                    if req_id and req_id in self.pending_requests:
                        self.pending_requests[req_id].set_result(data)
                    elif msg_type == "ws_message" and data.get("bridge_id") in self.ws_bridges:
                        bridge_ws = self.ws_bridges[data["bridge_id"]]
                        if not bridge_ws.closed:
                            await bridge_ws.send_str(data["data"])
                    elif msg_type == "ws_closed" and data.get("bridge_id") in self.ws_bridges:
                        bridge_ws = self.ws_bridges.pop(data["bridge_id"], None)
                        if bridge_ws and not bridge_ws.closed:
                            await bridge_ws.close()
                elif msg.type == WSMsgType.ERROR:
                    break
        finally:
            del self.tunnels[tunnel_id]
            print(f"[-] Tunnel {tunnel_id} disconnected")
        return ws

    def _get_tunnel_for_request(self, request):
        full_path = request.path
        parts = full_path.strip("/").split("/", 1)
        tunnel_id = parts[0] if parts else None

        if tunnel_id and tunnel_id in self.tunnels:
            path = "/" + parts[1] if len(parts) > 1 else "/"
            return self.tunnels[tunnel_id], path
        elif len(self.tunnels) == 1:
            tunnel_id = next(iter(self.tunnels))
            return self.tunnels[tunnel_id], full_path
        return None, None

    async def handle_proxy(self, request):
        # Check if this is a WebSocket upgrade request
        if request.headers.get("Upgrade", "").lower() == "websocket":
            return await self._handle_ws_bridge(request)

        tunnel_ws, path = self._get_tunnel_for_request(request)
        if not tunnel_ws:
            return web.Response(status=404, text="Tunnel not found.")

        req_id = str(uuid.uuid4())
        body = await request.read()

        future = asyncio.get_event_loop().create_future()
        self.pending_requests[req_id] = future

        await tunnel_ws.send_json({
            "type": "request",
            "req_id": req_id,
            "method": request.method,
            "path": path,
            "headers": dict(request.headers),
            "body": body.decode("utf-8", errors="replace"),
        })

        try:
            response_data = await asyncio.wait_for(future, timeout=60)
        except asyncio.TimeoutError:
            return web.Response(status=504, text="Tunnel timeout")
        finally:
            self.pending_requests.pop(req_id, None)

        return web.Response(
            status=response_data.get("status", 200),
            body=response_data.get("body", ""),
            headers=response_data.get("headers", {}),
        )

    async def _handle_ws_bridge(self, request):
        tunnel_ws, path = self._get_tunnel_for_request(request)
        if not tunnel_ws:
            return web.Response(status=502, text="No tunnel connected")

        browser_ws = web.WebSocketResponse()
        await browser_ws.prepare(request)

        bridge_id = str(uuid.uuid4())[:8]
        self.ws_bridges[bridge_id] = browser_ws

        # Tell tunnel client to open a WebSocket to local server
        query = request.query_string
        full_path = f"{path}?{query}" if query else path
        await tunnel_ws.send_json({
            "type": "ws_open",
            "bridge_id": bridge_id,
            "path": full_path,
        })

        try:
            async for msg in browser_ws:
                if msg.type == WSMsgType.TEXT:
                    await tunnel_ws.send_json({
                        "type": "ws_forward",
                        "bridge_id": bridge_id,
                        "data": msg.data,
                    })
                elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSED):
                    break
        finally:
            self.ws_bridges.pop(bridge_id, None)
            await tunnel_ws.send_json({
                "type": "ws_close",
                "bridge_id": bridge_id,
            })

        return browser_ws

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
        app.router.add_route("*", "/{path:.*}", self.handle_proxy)
        print(f"Tunnel server on port {self.public_port} | domain: {self.base_domain}")
        web.run_app(app, port=self.public_port)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Tunnel relay server")
    parser.add_argument("-p", "--port", type=int, default=int(os.environ.get("PORT", 9000)))
    parser.add_argument("-d", "--domain", default=os.environ.get("TUNNEL_DOMAIN", "localhost"))
    args = parser.parse_args()
    TunnelServer(public_port=args.port, base_domain=args.domain).run()
