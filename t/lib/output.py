#!/usr/bin/python3
from random import randrange as rr
import sys, time, json

def snmp_interface(i):
    data = {
        "name": "interface",
        "timestamp": int(time.time()),
        "fields": {
            "ifAdminStatus":     rr(2),
            "ifInDiscards":      rr(10),
            "ifInErrors":        rr(10),
            "ifHCInUcastPkts":   rr(10),
            "ifInOctets":        rr(10),
            "ifHCInOctets":      rr(10),
            "ifInUcastPkts":     rr(10),
            "ifInUnknownProtos": rr(10),
            "ifLastChange":      rr(100000000, 1000000000),
            "ifMtu":             rr(10),
            "ifOperStatus":      rr(2),
            "ifOutDiscards":     rr(10),
            "ifOutErrors":       rr(10),
            "ifHCOutUcastPkts":  rr(10),
            "ifOutOctets":       rr(10),
            "ifHCOutOctets":     rr(10),
            "ifOutQLen":         rr(10),
            "ifOutUcastPkts":    rr(10),
            "ifPhysAddress":     "aa:aa:aa:aa:aa:aa",
            "ifSpecific":        ".0.0",
            "ifSpeed":           rr(1000000000, 10000000000),
            "ifType":            161
        },
        "tags": {
            "agent_host": "tsds.telegraf.output.test.net",
            "host":       "test.host.net",
            "ifDescr":    "AggregateEthernet0",
            "ifIndex":    "0",
            "ifName":     'et-0/0/{}'.format(str(i))
        }
    }
    return data

def stream():
    while True:
        for i in range(26):
            print(json.dumps(snmp_interface(i)))
        time.sleep(60)

def dump_snmp():
    for i in range(26):
        print(json.dumps(snmp_interface(i)))

stream()
