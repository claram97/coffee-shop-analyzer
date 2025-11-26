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
    finishers = cp.getint("results", "workers", fallback=1)
    orchestrators = cp.getint("orchestrator", "workers", fallback=1)
    return {
        "filters": filters,
        "aggregators": aggregators,
        "joiners": joiners,
        "finishers": finishers,
        "orchestrators": orchestrators,
    }


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
    joiner = cp.getint("joiners", "routers", fallback=1)
    finisher = cp.getint("results", "routers", fallback=1)
    return {"fr_routers": filter, "j_routers": joiner, "results_routers": finisher}


def _read_election_ports(cp: configparser.ConfigParser):
    if "election_ports" not in cp:
        # Return defaults if section missing
        return {
            "filter_workers": 9100,
            "filter_routers": 9200,
            "aggregators": 9300,
            "joiner_workers": 9400,
            "joiner_routers": 9500,
            "results_workers": 9600,
            "results_routers": 9700,
        }
    
    ep = cp["election_ports"]
    return {
        "filter_workers": ep.getint("filter_workers", 9100),
        "filter_routers": ep.getint("filter_routers", 9200),
        "aggregators": ep.getint("aggregators", 9300),
        "joiner_workers": ep.getint("joiner_workers", 9400),
        "joiner_routers": ep.getint("joiner_routers", 9500),
        "results_workers": ep.getint("results_workers", 9600),
        "results_routers": ep.getint("results_routers", 9700),
    }


def cmd_workers(cp: configparser.ConfigParser, fmt: str):
    w = _read_workers(cp)
    if fmt == "plain":
        print(
            w["filters"],
            w["aggregators"],
            w["joiners"],
            w["finishers"],
            w["orchestrators"],
        )
    elif fmt == "env":
        print(f"FILTERS={w['filters']}")
        print(f"AGGREGATORS={w['aggregators']}")
        print(f"JOINERS={w['joiners']}")
        print(f"FINISHERS={w['finishers']}")
        print(f"ORCHESTRATORS={w['orchestrators']}")
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
        print(w["fr_routers"], w["j_routers"], w["results_routers"])
    elif fmt == "env":
        print(f"FR_ROUTERS={w['fr_routers']}")
        print(f"J_ROUTERS={w['j_routers']}")
        print(f"RESULTS_ROUTERS={w['results_routers']}")
    else:
        _die(f"unknown format: {fmt}")


def cmd_election_ports(cp: configparser.ConfigParser, fmt: str):
    ep = _read_election_ports(cp)
    if fmt == "plain":
        print(
            ep["filter_workers"],
            ep["filter_routers"],
            ep["aggregators"],
            ep["joiner_workers"],
            ep["joiner_routers"],
            ep["results_workers"],
            ep["results_routers"],
        )
    elif fmt == "env":
        print(f"ELECTION_PORT_FILTER_WORKERS={ep['filter_workers']}")
        print(f"ELECTION_PORT_FILTER_ROUTERS={ep['filter_routers']}")
        print(f"ELECTION_PORT_AGGREGATORS={ep['aggregators']}")
        print(f"ELECTION_PORT_JOINER_WORKERS={ep['joiner_workers']}")
        print(f"ELECTION_PORT_JOINER_ROUTERS={ep['joiner_routers']}")
        print(f"ELECTION_PORT_RESULTS_WORKERS={ep['results_workers']}")
        print(f"ELECTION_PORT_RESULTS_ROUTERS={ep['results_routers']}")
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
        f"FINISHERS={w['finishers']}",
        f"ORCHESTRATORS={w['orchestrators']}",
        f"FR_ROUTERS={r['fr_routers']}",
        f"J_ROUTERS={r['j_routers']}",
        f"RESULTS_ROUTERS={r['results_routers']}",
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
        "workers",
        help="Print worker counts (filters, aggregators, joiners, finishers, orchestrators)",
    )
    spw.add_argument("--format", choices=["plain", "env"], default="plain")

    spr = sub.add_parser(
        "routers", help="Print router counts (filter, joiner, finisher)"
    )
    spr.add_argument("--format", choices=["plain", "env"], default="env")

    spb = sub.add_parser("broker", help="Print RabbitMQ connection info")
    spb.add_argument("--format", choices=["plain", "env", "json"], default="env")

    spe = sub.add_parser("election_ports", help="Print election port configuration")
    spe.add_argument("--format", choices=["plain", "env"], default="env")

    sub.add_parser("all-env", help="Print all needed env vars for docker-compose")

    args = p.parse_args()
    cp = _load_cp(args.config)

    if args.cmd == "workers":
        cmd_workers(cp, args.format)
    elif args.cmd == "broker":
        cmd_broker(cp, args.format)
    elif args.cmd == "routers":
        cmd_routers(cp, args.format)
    elif args.cmd == "election_ports":
        cmd_election_ports(cp, args.format)
    elif args.cmd == "all-env":
        cmd_all_env(cp)
    else:
        _die(f"unknown command: {args.cmd}")


if __name__ == "__main__":
    main()
