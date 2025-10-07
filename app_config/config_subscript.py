#!/usr/bin/env python3
from __future__ import annotations

import argparse
import configparser
import json
import os
import sys


def _die(msg: str, code: int = 2):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def _load_cp(path: str) -> configparser.ConfigParser:
    path = os.path.abspath(path)
    if not os.path.exists(path):
        _die(f"INI file not found: {path}")
    cp = configparser.ConfigParser()
    cp.read(path)
    return cp


def _read_workers(cp: configparser.ConfigParser):
    filters = cp.getint("filters", "workers", fallback=1)
    aggregators = cp.getint("aggregators", "workers", fallback=1)
    joiners = cp.getint("joiners", "workers", fallback=1)
    return {"filters": filters, "aggregators": aggregators, "joiners": joiners}


def _read_broker(cp: configparser.ConfigParser):
    if "broker" not in cp:
        _die("Section [broker] missing in INI")
    b = cp["broker"]
    host = b.get("host", "localhost")
    port = b.getint("port", 5672)
    mgmt = b.getint("management_port", 15672)
    user = b.get("username", "guest")
    pwd = b.get("password", "guest")
    vhost = b.get("vhost", "/")
    return {
        "host": host,
        "port": port,
        "management_port": mgmt,
        "username": user,
        "password": pwd,
        "vhost": vhost,
    }


def _read_routers(cp: configparser.ConfigParser):
    filter = cp.getint("filters", "routers", fallback=1)
    return {"fr_routers": filter}


def cmd_workers(cp: configparser.ConfigParser, fmt: str):
    w = _read_workers(cp)
    if fmt == "plain":
        print(w["filters"], w["aggregators"], w["joiners"])
    elif fmt == "env":
        print(f"FILTERS={w['filters']}")
        print(f"AGGREGATORS={w['aggregators']}")
        print(f"JOINERS={w['joiners']}")
    else:
        _die(f"unknown format: {fmt}")


def cmd_broker(cp: configparser.ConfigParser, fmt: str):
    b = _read_broker(cp)
    if fmt == "plain":
        print(
            b["host"],
            b["port"],
            b["management_port"],
            b["username"],
            b["password"],
            b["vhost"],
        )
    elif fmt == "env":
        print(f"RABBIT_HOST={b['host']}")
        print(f"RABBIT_PORT={b['port']}")
        print(f"RABBIT_MGMT_PORT={b['management_port']}")
        print(f"RABBIT_USER={b['username']}")
        print(f"RABBIT_PASS={b['password']}")
        print(f"RABBIT_VHOST={b['vhost']}")
    elif fmt == "json":
        print(json.dumps(b, separators=(",", ":"), ensure_ascii=False))
    else:
        _die(f"unknown format: {fmt}")


def cmd_routers(cp: configparser.ConfigParser, fmt: str):
    w = _read_routers(cp)
    if fmt == "plain":
        print(w["fr_routers"])
    elif fmt == "env":
        print(f"FR_ROUTERS={w['fr_routers']}")
    else:
        _die(f"unknown format: {fmt}")


def cmd_all_env(cp: configparser.ConfigParser):
    w = _read_workers(cp)
    b = _read_broker(cp)
    r = _read_routers(cp)
    lines = [
        f"FILTERS={w['filters']}",
        f"AGGREGATORS={w['aggregators']}",
        f"JOINERS={w['joiners']}",
        f"FR_ROUTERS={r['fr_routers']}",
        f"RABBIT_HOST={b['host']}",
        f"RABBIT_PORT={b['port']}",
        f"RABBIT_MGMT_PORT={b['management_port']}",
        f"RABBIT_USER={b['username']}",
        f"RABBIT_PASS={b['password']}",
        f"RABBIT_VHOST={b['vhost']}",
    ]
    print("\n".join(lines))


def main():
    p = argparse.ArgumentParser(
        description="Tiny config subscript for bash/docker-compose"
    )
    p.add_argument(
        "-c",
        "--config",
        default="tables.ini",
        help="Path to INI file (default: tables.ini)",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    spw = sub.add_parser(
        "workers", help="Print worker counts (filters, aggregators, joiners)"
    )
    spw.add_argument("--format", choices=["plain", "env"], default="plain")

    spr = sub.add_parser("routers", help="Print router counts (filter)")
    spr.add_argument("--format", choices=["plain", "env"], default="env")

    spb = sub.add_parser("broker", help="Print RabbitMQ connection info")
    spb.add_argument("--format", choices=["plain", "env", "json"], default="env")

    sub.add_parser("all-env", help="Print all needed env vars for docker-compose")

    args = p.parse_args()
    cp = _load_cp(args.config)

    if args.cmd == "workers":
        cmd_workers(cp, args.format)
    elif args.cmd == "broker":
        cmd_broker(cp, args.format)
    elif args.cmd == "routers":
        cmd_routers(cp, args.format)
    elif args.cmd == "all-env":
        cmd_all_env(cp)
    else:
        _die(f"unknown command: {args.cmd}")


if __name__ == "__main__":
    main()
