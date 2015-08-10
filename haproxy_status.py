#!/usr/bin/env python

from BaseHTTPServer import BaseHTTPRequestHandler
from helpers.etcd import Etcd
from helpers.postgresql import Postgresql
import sys, yaml, socket

f = open(sys.argv[1], "r")
config = yaml.load(f.read())
f.close()

etcd = Etcd(config["etcd"])
postgresql = Postgresql(config["postgresql"])

class StatusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        return self.do_ANY()
    def do_OPTIONS(self):
        return self.do_ANY()
    def do_ANY(self):
        try:
            leader = etcd.current_leader()
            if leader and postgresql.name == leader["hostname"]:
              self.send_response(200)
            else:
              self.send_response(503)
            self.end_headers()
            self.wfile.write('\r\n')
        except socket.error:
            # we do not care for errors here
            pass
        return
    def log_message(self, format, *args):
        pass
    def finish(self):
        try:
            BaseHTTPRequestHandler.finish(self)
        except socket.error:
            # we do not care for errors here
            pass

try:
    from BaseHTTPServer import HTTPServer
    host, port = config["haproxy_status"]["listen"].split(":")
    server = HTTPServer((host, int(port)), StatusHandler)
    print 'listening on %s:%s' % (host, port)
    server.serve_forever()
except KeyboardInterrupt:
    print('^C received, shutting down server')
    server.socket.close()
