#!/usr/bin/env python3

import time
import socket
import requests
import time
import json
import dbm
import random
import dateutil.parser
import datetime
import sys
import logging
import logging.handlers
import typing
import yaml
import hashlib
import prometheus_client



class AquaTempConfig(typing.TypedDict):
    username: str
    password: str

class ATData(typing.TypedDict):
    incoming: float
    outgoing: float
    target: float
    on: bool


Gauge = prometheus_client.Gauge

Database: typing.TypeAlias = "dbm._Database"

class Metrics(typing.TypedDict):
    incoming: Gauge
    outgoing: Gauge
    target: Gauge
    on: Gauge

logger = logging.getLogger()

class Meter:

    db: Database
    at: AquaTempConfig

    def __init__(self) -> None:
        self.db = dbm.open("poolheatmetrics.db", "c")

        with open("config.yaml") as f:
            self.at  = yaml.safe_load(f)["aquatemp"]

        self.metrics = Metrics(
            incoming=Gauge("incoming", "Temperature incoming water", ["id"]),
            outgoing=Gauge(
                "outgoing", "Temperature outgoing water", [ "id"]
            ),
           target=Gauge(
                "target", "Target water temperature", [ "id"]
            ),
            on=Gauge(
                "on", "Enabled", [ "id"]
            ),        
            )

    def refresh_all_meters(self) -> None:
        t = []

        try:
            (token, id) = aquatemp_login(self.db, self.at)
            t = aquatemp_get_data(self.db, token, id)
        except SystemError:
            (token, id) = aquatemp_login(self.db, self.at, force=True)
            t = aquatemp_get_data(self.db, token, id)

        for p in t:
            self.metrics[p].labels(id=id).set(t[p])

def setup_logger(
    console_level: int = logging.DEBUG,
    file_level: int = logging.DEBUG,
    filename: str = "poolheatcontrol.log",
) -> None:
    h = logging.StreamHandler()
    h.setLevel(console_level)
    logger.addHandler(h)
    f = logging.handlers.TimedRotatingFileHandler(
        filename, when="midnight", backupCount=30
    )
    f.setFormatter(logging.Formatter("{asctime} - {levelname} - {message}", style="{"))
    f.setLevel(file_level)
    logger.addHandler(f)

    logger.setLevel(min(file_level, console_level))


def aquatemp_login(
    db: Database, at: AquaTempConfig, force: bool = False
) -> typing.Tuple[str, str]:
    key = "aquatemptokenandid"

    if not force and key in db:
        l = json.loads(db[key].decode("ascii"))
        (token, id) = l[0], l[1]
        return (token, id)

    md5 = hashlib.new("md5")
    md5.update(bytes(at["password"], "utf-8"))

    r = requests.request(
        method="POST",
        url="https://cloud.linked-go.com:449/crmservice/api/app/user/login",
        json={"userName": at["username"], "password": md5.hexdigest()},
        headers={"Content-Type": "application/json"},
    )

    if (not r.ok) or int(r.json()["error_code"]) != 0:
        raise SystemError("bad return from aquatemp login")

    token = str(r.json()["objectResult"]["x-token"])
    id = str(r.json()["objectResult"]["userId"])
    db[key] = json.dumps((token, id))
    return (token, id)


def aquatemp_get_device(db: Database, token: str, id: str) -> str:
    key = "aquatempdevice"

    if key in db:
        return db[key].decode("ascii")

    r = requests.request(
        method="POST",
        url="https://cloud.linked-go.com:449/crmservice/api/app/device/getMyAppectDeviceShareDataList",
        json={"toUser": id},
        headers={"Content-Type": "application/json", "x-token": token},
    )

    if (not r.ok) or int(r.json()["error_code"]) != 0:
        logger.debug(f"Bad return from aquatemp when fetching device: {r.text}")
        raise SystemError("bad return from aquatemp device check")

    device = str(r.json()["objectResult"][0]["deviceCode"])
    db[key] = device
    return device


def aquatemp_get_data(db: Database, token: str, id: str) -> ATData:
    device = aquatemp_get_device(db, token, id)

    # T02 is in, T03 is out, Set_Temp is target
#        json={"deviceCode": device, "protocalCodes": ["Set_Temp", "R02", "T02", "T03"]},

    r = requests.request(
        method="POST",
        url="https://cloud.linked-go.com:449/crmservice/api/app/device/getDataByCode",
        json={"deviceCode": device, "protocalCodes": ["R02", "T02", "T03","Power"]},
        headers={"Content-Type": "application/json", "x-token": token},
    )

    if (not r.ok) or int(r.json()["error_code"]) != 0:
        logger.debug(f"Bad return from aquatemp when fetching temperature: {r.text}")
        raise SystemError("bad return from aquatemp temperature fetch")

    d = ATData()

    for p in r.json()["objectResult"]:
        value = float(p["value"])
        key = "on"

        match p["code"]:
            case "R02":
                key = "target"
            case "T02":
                key = "incoming"
            case "T03":
                key = "outgoing"
            case "Power":
                key = "on"
                value = bool(float(p["value"]))

        d[key] = value

    return d

if __name__ == "__main__":
    setup_logger()


def serve() -> None:
    setup_logger()

    meter = Meter()
    prometheus_client.start_http_server(8023)
    meter.refresh_all_meters()

    while True:
        time.sleep(60)
        meter.refresh_all_meters()

if __name__ == "__main__":
    serve()
