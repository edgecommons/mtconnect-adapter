#!/usr/bin/env python3
"""SHDR feeder for the GREENGRASS deployed release leg.

A HARNESS, not product code. Serves the cppagent's two SHDR adapter ports (7401 -> MTC-E2E-001,
7402 -> MTC-E2E-002) from this dev machine, so real observation values flow through the agent
while the deployed component acquires from it.

Protocol: plain TCP, line-oriented, `|<dataItemId>|<value>` (empty leading timestamp = the agent
stamps arrival). Conditions use `|<id>|<level>|<nativeCode>|<severity>|<qualifier>|<text>`.

Manual injection: append lines to the command file (default /tmp/shdr-cmd, overridable via argv[1]);
each line is forwarded verbatim to every connected agent socket and the file is then truncated.
"""

import socket
import threading
import time
import os
import sys

CMD_FILE = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    os.environ.get("TEMP", "/tmp"), "shdr-cmd")

_socks = []
_lock = threading.Lock()


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def broadcast(line):
    data = (line + "\n").encode()
    with _lock:
        dead = []
        for s in _socks:
            try:
                s.sendall(data)
            except OSError:
                dead.append(s)
        for s in dead:
            _socks.remove(s)


def serve(port, prefix):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(4)
    log(f"SHDR listening on 0.0.0.0:{port} ({prefix})")
    while True:
        sock, addr = srv.accept()
        log(f"agent connected on {port} from {addr}")
        sock.sendall(f"|{prefix}-avail|AVAILABLE\n".encode())
        sock.sendall(f"|{prefix}-exec|READY\n".encode())
        sock.sendall(f"|{prefix}-Xabs|0.0\n".encode())
        sock.sendall(f"|{prefix}-travel|NORMAL||||\n".encode())
        with _lock:
            _socks.append(sock)
        threading.Thread(target=drain, args=(sock, port), daemon=True).start()


def drain(sock, port):
    """The agent may send PING; answer PONG so it does not drop us as unresponsive."""
    try:
        f = sock.makefile("rb")
        for raw in f:
            line = raw.decode(errors="replace").strip()
            if line.startswith("* PING"):
                sock.sendall(b"* PONG 10000\n")
    except OSError:
        pass
    finally:
        log(f"agent disconnected on {port}")
        with _lock:
            if sock in _socks:
                _socks.remove(sock)


def commands():
    while True:
        try:
            if os.path.exists(CMD_FILE) and os.path.getsize(CMD_FILE) > 0:
                with open(CMD_FILE, "r+") as fh:
                    lines = [ln.strip() for ln in fh if ln.strip()]
                    fh.seek(0)
                    fh.truncate()
                for ln in lines:
                    log(f"INJECT {ln}")
                    broadcast(ln)
        except OSError:
            pass
        time.sleep(0.25)


def main():
    for port, prefix in ((7401, "d1"), (7402, "d2")):
        threading.Thread(target=serve, args=(port, prefix), daemon=True).start()
    threading.Thread(target=commands, daemon=True).start()
    log(f"command file: {CMD_FILE}")

    x = 0.0
    tick = 0
    while True:
        x = round((x + 0.5) % 40.0, 2)
        tick += 1
        broadcast(f"|d1-Xabs|{x}")
        broadcast(f"|d2-Xabs|{round(x / 2, 2)}")
        if tick % 8 == 0:
            broadcast(f"|d1-exec|{'ACTIVE' if (tick // 8) % 2 else 'READY'}")
        if tick % 20 == 0:
            log(f"heartbeat: d1-Xabs={x}")
        time.sleep(0.5)


if __name__ == "__main__":
    main()
