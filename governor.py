#!/usr/bin/env python

import sys, os, yaml, time, urllib2, atexit
import logging

from helpers.etcd import Etcd
from helpers.postgresql import Postgresql
from helpers.ha import Ha


logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)

f = open(sys.argv[1], "r")
config = yaml.load(f.read())
f.close()

etcd = Etcd(config["etcd"])
postgresql = Postgresql(config["postgresql"])
ha = Ha(postgresql, etcd)

# wait for etcd to be available
etcd_ready = False
while not etcd_ready:
    try:
        etcd.touch_member(postgresql.name, postgresql.connection_string)
        etcd_ready = True
    except urllib2.URLError:
        logging.info("waiting on etcd")
        time.sleep(5)

# is data directory empty?
if postgresql.data_directory_empty():
    # racing to initialize
    if etcd.race("/initialize", postgresql.name):
        postgresql.initialize()
        etcd.take_leader(postgresql.name)
        postgresql.start()
    else:
        synced_from_leader = False
        while not synced_from_leader:
            leader = etcd.current_leader()
            if not leader:
                time.sleep(5)
                continue
            if postgresql.sync_from_leader(leader):
                postgresql.write_recovery_conf(leader)
                # Give the leader time to create the replication slot
                time.sleep(config["loop_wait"] * 2)
                postgresql.start()
                synced_from_leader = True
            else:
                time.sleep(5)
else:
    leader = etcd.current_leader()
    if leader is not None:
        if leader['hostname'] == postgresql.name:
            # still a leader
            postgresql.start()
        else:
            postgresql.follow_the_leader(leader)
    else:
        postgresql.follow_no_leader()
    postgresql.start()


while True:
    etcd.touch_member(postgresql.name, postgresql.connection_string)
    logging.info(ha.run_cycle())

    # create replication slots
    if postgresql.is_leader():
        for member in etcd.members():
            if member['hostname'] != postgresql.name:
                postgresql.query("""
                    DO LANGUAGE plpgsql $$
                    DECLARE somevar VARCHAR;
                    BEGIN
                        SELECT slot_name INTO somevar
                        FROM pg_replication_slots
                        WHERE slot_name = '%(slot)s'
                        LIMIT 1;
                        IF NOT FOUND THEN
                            PERFORM pg_create_physical_replication_slot('%(slot)s');
                        END IF;
                    END$$;""" % {"slot": member['hostname']})

    time.sleep(config["loop_wait"])
