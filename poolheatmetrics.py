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
import zeroconf

PUMPNAME = "Poolpump"
MAX_WAIT = 150


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
    pumprunning: Gauge


logger = logging.getLogger()


class HueController(zeroconf.ServiceListener):
    # Only handle one bridge for now
    _url = None

    def update_service(self, zc: zeroconf.Zeroconf, type_: str, name: str) -> None:
        self.add_service(zc, type_, name)

    def remove_service(self, zc: zeroconf.Zeroconf, type_: str, name: str) -> None:
        self._url = None

    def add_service(self, zc: zeroconf.Zeroconf, type_: str, name: str) -> None:
        info = typing.cast(zeroconf.ServiceInfo, zc.get_service_info(type_, name))
        host = socket.inet_ntoa(info.addresses[0])

        proto = "http"
        if info.port == 443:
            proto = "https"
        self._url = f"{proto}://{host}"
        logger.debug(f"Noticed Hue Controller at {self._url}")

    @property
    def url(self) -> str:
        return typing.cast(str, self._url)



class Meter:

    db: Database
    at: AquaTempConfig
    url: str
    hue_id: str
    pump: str

    def __init__(self) -> None:
        self.db = dbm.open("poolheatmetrics.db", "c")

        self.find_hue()
        self.auth_hue()
        self.find_pump()

        with open("config.yaml") as f:
            self.at = yaml.safe_load(f)["aquatemp"]

        self.metrics = Metrics(
            incoming=Gauge("incoming", "Temperature incoming water", ["id"]),
            outgoing=Gauge("outgoing", "Temperature outgoing water", ["id"]),
            target=Gauge("target", "Target water temperature", ["id"]),
            on=Gauge("on", "Enabled", ["id"]),
            pumprunning=Gauge("pumprunning", "Pump is running", ["name"]),
        )

    def find_hue(self) -> None:
        "Find a Hue locally through zeroconf"
        zc = zeroconf.Zeroconf()
        listener = HueController()
        _browser = zeroconf.ServiceBrowser(zc, "_hue._tcp.local.", listener)

        count = 0
        while count < MAX_WAIT and not listener.url:
            time.sleep(1)
        zc.close()

        self.url = listener.url
        if not self.url:
            raise SystemExit("Did not found Hue bridge")

    def auth_hue(self) -> None:
        if not "hue_id" in self.db:
            data = {"devicetype": "Pump metrics"}
            r = requests.post(f"{self.url}/api", json=data, verify=False)
            if r.status_code == 200:
                for p in r.json():
                    if "success" in p:
                        self.db["hue_id"] = bytes(p["success"]["username"], "ascii")

        if "hue_id" not in self.db:
            raise SystemError("No user in hue")

        id = self.db["hue_id"]
        hue_id = id.decode()

        logger.debug(f"Found hue id {hue_id}")
        self.hue_id = hue_id

    def find_pump(self) -> None:
        r = requests.get(f"{self.url}/api/{self.hue_id}", verify=False)
        if r.status_code != 200:
            raise SystemError("Getting Hue status failed")
        hue = r.json()
        for p in hue["lights"]:
            if hue["lights"][p]["name"] == PUMPNAME:
                logger.debug(f"Found pump {PUMPNAME}")
                self.pump = p
                return
        raise SystemError(f"{PUMPNAME} not found in list of controlled units")

    def is_running(self) -> bool:
        r = requests.get(
            f"{self.url}/api/{self.hue_id}/lights/{self.pump}", verify=False
        )
        if r.status_code != 200:
            raise SystemError("Getting Hue pumpstatus failed")
        hue = r.json()
        return hue["state"]["on"]

    def refresh_all_meters(self) -> None:
    
        try:
            (token, id) = aquatemp_login(self.db, self.at)
            t = aquatemp_get_data(self.db, token, id)
        except SystemError:
            (token, id) = aquatemp_login(self.db, self.at, force=True)
            t = aquatemp_get_data(self.db, token, id)

        for p in t.keys():
            self.metrics[p].labels(id=id).set(t[p]) # type:ignore

        self.metrics["pumprunning"].labels(name=PUMPNAME).set(self.is_running())

def setup_logger(
    console_level: int = logging.DEBUG,
    file_level: int = logging.DEBUG,
    filename: str = "poolheatmetrics.log",
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
        json={"deviceCode": device, "protocalCodes": ["R02", "T02", "T03", "Power"]},
        headers={"Content-Type": "application/json", "x-token": token},
    )

    if (not r.ok) or int(r.json()["error_code"]) != 0:
        logger.debug(f"Bad return from aquatemp when fetching temperature: {r.text}")
        raise SystemError("bad return from aquatemp temperature fetch")

    d = ATData(target=-1, incoming=-1, outgoing=-1, on=False)

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

        d[key] = value # type:ignore

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
