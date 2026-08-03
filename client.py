import asyncio
import json
import sys
import os
import ssl
import argparse
import certifi
import aiohttp
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

DEFAULT_SERVER = os.environ.get("TUNNEL_SERVER", "ws://localhost:9000/ws")


def create_ssl_context():
    ctx = ssl.create_default_context(cafile=certifi.where())
    return ctx


STATIC_EXTENSIONS = {
    ".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico",
    ".woff", ".woff2", ".ttf", ".eot", ".map", ".json", ".webp",
    ".mp4", ".webm", ".ogg", ".mp3", ".wav", ".pdf", ".html", ".htm",
}


def is_static_asset(path):
    import os
    ext = os.path.splitext(path.split("?")[0])[1].lower()
    return ext in STATIC_EXTENSIONS


class TunnelConnection:
    def __init__(self, local_port, server_url, session, ssl_ctx, spa=False):
        self.local_port = local_port
        self.server_url = server_url
        self.session = session
        self.ssl_ctx = ssl_ctx
        self.spa = spa
        self.public_url = None
        self.ws = None
        self.reconnect = True

    async def forward_request(self, data):
        url = f"http://localhost:{self.local_port}{data['path']}"
        headers = {k: v for k, v in data.get("headers", {}).items()
                   if k.lower() not in ("host", "connection", "upgrade")}
        try:
            async with self.session.request(data["method"], url, headers=headers, data=data.get("body")) as resp:
                resp_body = await resp.text()
                status = resp.status

                if self.spa and status == 404 and not is_static_asset(data["path"]):
                    fallback_url = f"http://localhost:{self.local_port}/"
                    async with self.session.request("GET", fallback_url, headers=headers) as fallback_resp:
                        resp_body = await fallback_resp.text()
                        status = fallback_resp.status
                        return {
                            "req_id": data["req_id"],
                            "status": status,
                            "body": resp_body,
                            "headers": {k: v for k, v in fallback_resp.headers.items()
                                        if k.lower() not in ("transfer-encoding", "content-encoding")},
                        }

                return {
                    "req_id": data["req_id"],
                    "status": status,
                    "body": resp_body,
                    "headers": {k: v for k, v in resp.headers.items()
                                if k.lower() not in ("transfer-encoding", "content-encoding")},
                }
        except Exception as e:
            return {"req_id": data["req_id"], "status": 502, "body": str(e), "headers": {}}

    async def connect(self):
        while self.reconnect:
            try:
                self.ws = await self.session.ws_connect(self.server_url, ssl=self.ssl_ctx)
                async for msg in self.ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if data["type"] == "registered":
                            self.public_url = data["url"]
                            print(f"  {self.local_port} → {self.public_url}")
                        elif data["type"] == "request":
                            response = await self.forward_request(data)
                            await self.ws.send_json(response)
                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        break
            except aiohttp.ClientError as e:
                print(f"  {self.local_port} → connection failed: {e}")

            if self.reconnect:
                print(f"  {self.local_port} → reconnecting in 3s...")
                await asyncio.sleep(3)

    async def disconnect(self):
        self.reconnect = False
        if self.ws and not self.ws.closed:
            await self.ws.close()


class TunnelClient:
    def __init__(self, server_url):
        self.server_url = server_url
        self.connections = {}
        self.session = None
        self.ssl_ctx = None
        self.running = True

    async def add_port(self, port, spa=False):
        if port in self.connections:
            print(f"  Port {port} already connected")
            return
        conn = TunnelConnection(port, self.server_url, self.session, self.ssl_ctx, spa=spa)
        self.connections[port] = conn
        mode = " (SPA)" if spa else ""
        asyncio.create_task(conn.connect())
        if spa:
            print(f"  Port {port} registered as SPA (fallback to index.html)")

    async def remove_port(self, port):
        if port not in self.connections:
            print(f"  Port {port} not connected")
            return
        conn = self.connections.pop(port)
        await conn.disconnect()
        print(f"  Port {port} disconnected")

    def list_ports(self):
        if not self.connections:
            print("  No active tunnels")
            return
        print("  Active tunnels:")
        for port, conn in self.connections.items():
            spa_tag = " [SPA]" if conn.spa else ""
            print(f"    {port}{spa_tag} → {conn.public_url or 'connecting...'}")

    async def command_loop(self):
        loop = asyncio.get_event_loop()
        while self.running:
            try:
                line = await loop.run_in_executor(None, lambda: input("\ntunnel> "))
            except (EOFError, KeyboardInterrupt):
                self.running = False
                break

            parts = line.strip().split()
            if not parts:
                continue

            cmd = parts[0].lower()

            if cmd == "add" and len(parts) >= 2:
                spa = "--spa" in parts
                ports_to_add = [p for p in parts[1:] if p != "--spa"]
                for p in ports_to_add:
                    try:
                        await self.add_port(int(p), spa=spa)
                    except ValueError:
                        print(f"  Invalid port: {p}")
            elif cmd == "remove" and len(parts) >= 2:
                for p in parts[1:]:
                    try:
                        await self.remove_port(int(p))
                    except ValueError:
                        print(f"  Invalid port: {p}")
            elif cmd == "list":
                self.list_ports()
            elif cmd in ("quit", "exit"):
                self.running = False
            elif cmd == "help":
                print("  Commands:")
                print("    add <port> [--spa]       Expose local port (--spa for SPA fallback)")
                print("    remove <port> [port ...] Stop exposing port(s)")
                print("    list                     Show active tunnels")
                print("    quit                     Exit")
            else:
                print("  Unknown command. Type 'help' for usage.")

    async def run(self, initial_ports):
        self.ssl_ctx = create_ssl_context() if self.server_url.startswith("wss://") else None
        self.session = aiohttp.ClientSession()
        print(f"Server: {self.server_url}")
        print("Type 'help' for commands.\n")

        for port, spa in initial_ports:
            await self.add_port(port, spa=spa)

        await self.command_loop()

        for conn in self.connections.values():
            await conn.disconnect()
        await self.session.close()
        print("Bye.")


def main():
    parser = argparse.ArgumentParser(
        prog="tunnel",
        description="Expose local ports to the internet",
        usage="python client.py [-s SERVER] [--spa PORTS] [PORT ...]"
    )
    parser.add_argument("ports", nargs="*", type=int, help="Initial port(s) to expose")
    parser.add_argument("-s", "--server", default=DEFAULT_SERVER, help="Tunnel server URL (default: from .env)")
    parser.add_argument("--spa", nargs="*", type=int, default=[], metavar="PORT", help="Ports to treat as SPA (fallback to index.html)")
    args = parser.parse_args()

    all_ports = [(p, p in args.spa) for p in args.ports]
    for p in args.spa:
        if p not in args.ports:
            all_ports.append((p, True))

    client = TunnelClient(server_url=args.server)
    asyncio.run(client.run(all_ports))


if __name__ == "__main__":
    main()
