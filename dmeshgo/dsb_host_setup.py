#!/usr/bin/env python3
"""Generate ~/hotelres-dmesh/config.json with docker bridge IPs of the infra
containers, so bare-metal service binaries can reach mongo/memcached/consul."""
import json
import os
import subprocess

os.chdir(os.path.expanduser("~/hotelres-dmesh"))

def ip(name):
    out = subprocess.check_output(
        ["docker", "inspect", "-f",
         "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", name],
        text=True).strip()
    assert out, "no IP for " + name
    return out

def c(n):
    cid = subprocess.check_output(
        ["docker", "compose", "ps", "-q", n], text=True).strip().splitlines()
    assert cid and cid[0], "no container for " + n
    return cid[0]

cfg = {
    "consulAddress": ip(c("consul")) + ":8500",
    "jaegerAddress": ip(c("jaeger")) + ":6831",
    "FrontendPort": "5000",
    "GeoPort": "8083",
    "GeoMongoAddress": ip(c("mongodb-geo")) + ":27017",
    "ProfilePort": "8081",
    "ProfileMongoAddress": ip(c("mongodb-profile")) + ":27017",
    "ProfileMemcAddress": ip(c("memcached-profile")) + ":11211",
    "ReviewPort": "8088",
    "ReviewMongoAddress": ip(c("mongodb-review")) + ":27017",
    "ReviewMemcAddress": ip(c("memcached-review")) + ":11211",
    "AttractionsPort": "8089",
    "AttractionsMongoAddress": ip(c("mongodb-attractions")) + ":27017",
    "RatePort": "8084",
    "RateMongoAddress": ip(c("mongodb-rate")) + ":27017",
    "RateMemcAddress": ip(c("memcached-rate")) + ":11211",
    "RecommendPort": "8085",
    "RecommendMongoAddress": ip(c("mongodb-recommendation")) + ":27017",
    "ReservePort": "8087",
    "ReserveMongoAddress": ip(c("mongodb-reservation")) + ":27017",
    "ReserveMemcAddress": ip(c("memcached-reserve")) + ":11211",
    "SearchPort": "8082",
    "UserPort": "8086",
    "UserMongoAddress": ip(c("mongodb-user")) + ":27017",
    "KnativeDomainName": "",
}
json.dump(cfg, open("config.json", "w"), indent=2)
print("config.json written; consul at", cfg["consulAddress"])
