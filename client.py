import asyncio
import json
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


class TunnelConnection:
    def __init__(self, local_port, server_url, session, ssl_ctx):
        self.local_port = local_port
        self.server_url = server_url
        self.session = session
        self.ssl_ctx = ssl_ctx
        self.public_url = None
        self.tunnel_id = None
        self.ws = None
        self.reconnect = True

    async def forward_request(self, data):
        url = f"http://localhost:{self.local_port}{data['path']}"
        headers = {k: v for k, v in data.get("headers", {}).items()
                   if k.lower() not in ("host", "connection", "upgrade")}
        try:
            async with self.session.request(data["method"], url, headers=headers, data=data.get("body")) as resp:
                resp_body = await resp.read()
                return {
                    "req_id": data["req_id"],
                    "status": resp.status,
                    "body": resp_body.decode("utf-8", errors="replace"),
                    "headers": {k: v for k, v in resp.headers.items()
                                if k.lower() not in ("transfer-encoding", "content-encoding", "content-length")},
                }
        except Exception as e:
            return {"req_id": data["req_id"], "status": 502, "body": str(e), "headers": {}}

    async def connect(self):
        while self.reconnect:
            try:
                url = self.server_url
                if self.tunnel_id:
                    sep = "&" if "?" in url else "?"
                    url = f"{url}{sep}id={self.tunnel_id}"
                self.ws = await self.session.ws_connect(url, ssl=self.ssl_ctx, heartbeat=20)
                async for msg in self.ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if data["type"] == "registered":
                            self.public_url = data["url"]
                            self.tunnel_id = data.get("tunnel_id")
                            print(f"  {self.local_port} → {self.public_url}")
                        elif data["type"] == "request":
                            response = await self.forward_request(data)
                            try:
                                await self.ws.send_json(response)
                            except (ConnectionError, aiohttp.ClientError):
                                break
                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        break
            except (aiohttp.ClientError, ConnectionError, OSError) as e:
                print(f"  {self.local_port} → connection lost: {e}")

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

    async def add_port(self, port):
        if port in self.connections:
            print(f"  Port {port} already connected")
            return
        conn = TunnelConnection(port, self.server_url, self.session, self.ssl_ctx)
        self.connections[port] = conn
        asyncio.create_task(conn.connect())

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
            print(f"    {port} → {conn.public_url or 'connecting...'}")

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
                for p in parts[1:]:
                    try:
                        await self.add_port(int(p))
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
                print("    add <port> [port ...]    Expose local port(s)")
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

        for port in initial_ports:
            await self.add_port(port)

        await self.command_loop()

        for conn in self.connections.values():
            await conn.disconnect()
        await self.session.close()
        print("Bye.")


def main():
    parser = argparse.ArgumentParser(
        prog="tunnel",
        description="Expose local ports to the internet",
        usage="python client.py [PORT ...]"
    )
    parser.add_argument("ports", nargs="*", type=int, help="Local port(s) to expose")
    parser.add_argument("-s", "--server", default=DEFAULT_SERVER, help="Tunnel server URL (default: from .env)")
    args = parser.parse_args()

    client = TunnelClient(server_url=args.server)
    asyncio.run(client.run(args.ports))


if __name__ == "__main__":
    main()
