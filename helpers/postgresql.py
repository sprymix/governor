import os, psycopg2, re, time
import logging
import shutil

from urlparse import urlparse


logger = logging.getLogger(__name__)

class Postgresql:

    def __init__(self, config):
        self.name = config["name"]
        self.params = config.get('parameters', {})
        self.listen_addrs = self.params.get('listen_addresses', 'localhost') \
                                       .split(',')
        self.host = self.listen_addrs[0]
        self.port = self.params.get('port', 5432)
        self.data_dir = config["data_dir"]
        self.replication = config["replication"]

        self.config = config

        self.cursor_holder = None
        self.connection_string = "postgres://%s:%s@%s:%s/postgres" % (self.replication["username"], self.replication["password"], self.host, self.port)

        self.conn = None

    def cursor(self):
        if not self.cursor_holder:
            self.conn = psycopg2.connect("postgres://%s:%s/postgres" % (self.host, self.port))
            self.conn.autocommit = True
            self.cursor_holder = self.conn.cursor()

        return self.cursor_holder

    def disconnect(self):
        try:
            self.conn.close()
        except Exception as e:
            logger.error("Error disconnecting: %s" % e)

    def query(self, sql):
        max_attempts = 0
        while True:
            try:
                self.cursor().execute(sql)
                break
            except psycopg2.OperationalError as e:
                if self.conn:
                    self.disconnect()
                self.cursor_holder = None
                if max_attempts > 4:
                    raise e
                max_attempts += 1
                time.sleep(5)
        return self.cursor()

    def data_directory_empty(self):
        return not os.path.exists(self.data_dir) or os.listdir(self.data_dir) == []

    def drop_cluster(self):
        shutil.rmtree(self.data_dir)
        os.mkdir(self.data_dir, 0700)

    def initialize(self):
        logger.info("Initializing cluster in %s" % self.data_dir)
        if os.system("initdb -D %s" % self.data_dir) == 0:
            # start Postgres without options to setup replication user indepedent of other system settings
            self.write_pg_hba()
            os.system("pg_ctl start -w -D %s -o '%s'" % (self.data_dir, self.server_options()))
            self.create_replication_user()
            os.system("pg_ctl stop -w -m fast -D %s" % self.data_dir)

            return True
        else:
            logger.error("Could not initialize cluster in %s" % self.data_dir)

            return False

    def sync_from_leader(self, leader):
        leader = urlparse(leader["address"])
        logger.info("Syncing base backup from leader %s" % leader.hostname)

        f = open("./pgpass", "w")
        f.write("%(hostname)s:%(port)s:*:%(username)s:%(password)s\n" %
                {"hostname": leader.hostname, "port": leader.port, "username": leader.username, "password": leader.password})
        f.close()

        os.system("chmod 600 pgpass")

        return os.system("PGPASSFILE=pgpass pg_basebackup -R -D %(data_dir)s --host=%(host)s --port=%(port)s -U %(username)s" %
                {"data_dir": self.data_dir, "host": leader.hostname, "port": leader.port, "username": leader.username}) == 0

    def is_leader(self):
        return not self.query("SELECT pg_is_in_recovery();").fetchone()[0]

    def is_running(self):
        return os.system("pg_ctl status -D %s > /dev/null" % self.data_dir) == 0

    def start(self):
        if self.is_running():
            logger.error("Cannot start PostgreSQL because one is already running.")
            return False

        pid_path = "%s/postmaster.pid" % self.data_dir
        if os.path.exists(pid_path):
            os.remove(pid_path)
            logger.info("Removed %s" % pid_path)

        return os.system("pg_ctl start -w -D %s -o '%s'" % (self.data_dir, self.server_options())) == 0

    def stop(self, mode='fast'):
        return os.system("pg_ctl stop -w -D %s -m %s -w" % (self.data_dir, mode)) != 0

    def reload(self):
        return os.system("pg_ctl reload -w -D %s" % self.data_dir) == 0

    def restart(self):
        return os.system("pg_ctl restart -w -D %s -m fast" % self.data_dir) == 0

    def server_options(self):
        options = "-c listen_addresses=%s -c port=%s" % (self.host, self.port)
        for setting, value in self.config["parameters"].iteritems():
            options += " -c \"%s=%s\"" % (setting, value)
        return options

    def is_healthy(self):
        if not self.is_running():
            logger.warning("Postgresql is not running.")
            return False

        if self.is_leader():
            return True

        return True

    def is_healthiest_node(self, state_store):
        # this should only happen on initialization
        if state_store.last_leader_operation() is None:
            return True

        if (state_store.last_leader_operation() - self.xlog_position()) > self.config["maximum_lag_on_failover"]:
            return False

        for member in state_store.members():
            if member["hostname"] == self.name:
                continue
            try:
                member_conn = psycopg2.connect(member["address"])
                member_conn.autocommit = True
                member_cursor = member_conn.cursor()
                member_cursor.execute("SELECT %s - (pg_last_xlog_replay_location() - '0/000000'::pg_lsn) AS bytes;" % self.xlog_position())
                xlog_diff = member_cursor.fetchone()[0]
                logger.info([self.name, member["hostname"], xlog_diff])
                if xlog_diff < 0:
                    member_cursor.close()
                    return False
                member_cursor.close()
            except psycopg2.OperationalError:
                continue
        return True

    def replication_slot_name(self):
        member = os.environ.get("MEMBER")
        (member, _) = re.subn(r'[^a-z0-9]+', r'_', member)
        return member

    def write_pg_hba(self):
        pg_hba = self.params.get('hba_file',
                                 '{}/pg_hba.conf'.format(self.data_dir))
        with open(pg_hba, "a") as f:
            f.write("host replication %(username)s %(network)s md5" %
                    {"username": self.replication["username"],
                     "network": self.replication["network"]})

            for entry in self.config.get('hba_entries') or []:
                f.write('\n')
                f.write(entry)

    def write_recovery_conf(self, leader_hash):
        f = open("%s/recovery.conf" % self.data_dir, "w")
        f.write("""
standby_mode = 'on'
primary_slot_name = '%(recovery_slot)s'
recovery_target_timeline = 'latest'
""" % {"recovery_slot": self.name})
        if leader_hash is not None:
            leader = urlparse(leader_hash["address"])
            f.write("""
primary_conninfo = 'user=%(user)s password=%(password)s host=%(hostname)s port=%(port)s sslmode=prefer sslcompression=1'
            """ % {"user": leader.username, "password": leader.password, "hostname": leader.hostname, "port": leader.port})

        if "recovery_conf" in self.config:
            for name, value in self.config["recovery_conf"].iteritems():
                f.write("%s = '%s'\n" % (name, value))
        f.close()

    def follow_the_leader(self, leader_hash):
        leader = urlparse(leader_hash["address"])
        if os.system("grep 'host=%(hostname)s port=%(port)s' %(data_dir)s/recovery.conf > /dev/null" % {"hostname": leader.hostname, "port": leader.port, "data_dir": self.data_dir}) != 0:
            self.write_recovery_conf(leader_hash)
            self.restart()
        return True

    def follow_no_leader(self):
        if not os.path.exists("%s/recovery.conf" % self.data_dir) or os.system("grep primary_conninfo %(data_dir)s/recovery.conf &> /dev/null" % {"data_dir": self.data_dir}) == 0:
            self.write_recovery_conf(None)
            if self.is_running():
                self.restart()
        return True

    def promote(self):
        return os.system("pg_ctl promote -w -D %s" % self.data_dir) == 0

    def demote(self, state_store, leader):
        logger.info("Stopping server")
        if not self.stop('fast'):
            self.stop('immediate')
        logger.info("Dropping cluster")
        self.drop_cluster()
        logger.info("Syncing from leader")
        self.sync_from_leader(leader)
        self.write_recovery_conf(leader)
        # Make sure we are present in members list so that
        # a proper replication slot is created.
        state_store.touch_member(self.name, self.connection_string)
        time.sleep(5)
        self.start()

    def create_replication_user(self):
        self.query("CREATE USER \"%s\" WITH REPLICATION ENCRYPTED PASSWORD '%s';" % (self.replication["username"], self.replication["password"]))

    def xlog_position(self):
        return self.query("SELECT pg_last_xlog_replay_location() - '0/0000000'::pg_lsn;").fetchone()[0] or 0

    def last_operation(self):
        return self.query("SELECT pg_current_xlog_location() - '0/00000'::pg_lsn;").fetchone()[0]
