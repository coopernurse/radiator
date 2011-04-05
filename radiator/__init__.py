
import uuid
import struct
import os
import time
import re
import types
import base64
import tempfile
import zlib
from collections import deque

from gevent.pool import Pool
from gevent.server import StreamServer
from stomp import StompServer

def start_server(bind_addr, port, pool_size=5000, dir=None, fsync_millis=0):
    broker = Broker(dir=dir, fsync_millis=fsync_millis)
    pool   = Pool(pool_size)
    server = StreamServer((bind_addr, port),
                          lambda sock, addr: StompServer(sock, broker).drain(),
                          spawn=pool)
    server.serve_forever()

class Session(object):

    def __init__(self, session_id, on_message_cb):
        self.session_id       = session_id
        self.on_message_cb    = on_message_cb
        self.subscribed_dests = { }
        self.order_of_dequeue = [ ]
        self.busy = False

    def subscribe(self, dest_name, auto_ack):
        if not self.subscribed_dests.has_key(dest_name):
            self.order_of_dequeue.append(dest_name)
        self.subscribed_dests[dest_name] = auto_ack
        #self._dump("subscribe %s" % dest_name)

    def unsubscribe(self, dest_name):
        if self.subscribed_dests.has_key(dest_name):
            del(self.subscribed_dests[dest_name])
            self.order_of_dequeue.remove(dest_name)
        #self._dump("unsubscribe %s" % dest_name)

    def get_auto_ack(self, dest_name):
        return self.subscribed_dests[dest_name]

    def process_msg(self, dest_name, message_id, body):
        self.busy = True
        # move dest to end of list so we try to balance
        # work evenly across queues
        self.order_of_dequeue.remove(dest_name)
        self.order_of_dequeue.append(dest_name)
        self.on_message_cb(dest_name, message_id, body)

    def _dump(self, msg):
        print "msg: %s\t%s\t%s" % \
              (msg, str(self.subscribed_dests), str(self.order_of_dequeue))

class Broker(object):

    def __init__(self, dir=None, fsync_millis=0):
        self.dir = dir
        self.fsync_millis = fsync_millis
        #
        #   key: dest_name
        # value: Dest obj (provides send(), receive(), ack())
        self.dest_dict        = { }
        #
        #   key: dest_name
        # value: list of session_ids subscribed to dest_name
        self.dest_subscribers = { }
        #
        #   key: session_id
        # value: Session obj
        self.session_dict     = { }

    def send(self, dest_name, body):
        self._get_or_create_dest(dest_name).send(body)
        for session_id in self.dest_subscribers[dest_name]:
            session = self.session_dict[session_id]
            if not session.busy:
                self._send_msg_to_session(session)
                self.dest_subscribers[dest_name].rotate(-1)
                break

    def subscribe(self, dest_name, auto_ack, session_id, on_message_cb):
        session = self._get_or_create_session(session_id, on_message_cb)
        session.subscribe(dest_name, auto_ack)
        self._get_or_create_dest(dest_name)
        if not session_id in self.dest_subscribers[dest_name]:
            self.dest_subscribers[dest_name].append(session_id)
        self._send_msg_to_session(session)

    def unsubscribe(self, dest_name, session_id):
        self._get_or_create_dest(dest_name)
        if self.session_dict.has_key(session_id):
            self.session_dict[session_id].unsubscribe(dest_name)
        if session_id in self.dest_subscribers[dest_name]:
            self.dest_subscribers[dest_name].remove(session_id)

    def ack(self, session_id, message_id):
        (message_id, dest_name) = message_id.split(",")
        self._get_or_create_dest(dest_name).ack(uuid.UUID(message_id))
        if self.session_dict.has_key(session_id):
            session = self.session_dict[session_id]
            session.busy = False
            self._send_msg_to_session(session)

    def _send_msg_to_session(self, session):
        if not session.busy:
            for dest_name in session.order_of_dequeue:
                dest = self._get_or_create_dest(dest_name)
                msg = dest.receive(session.get_auto_ack(dest_name))
                if msg:
                    message_id = "%s,%s" % (msg[1], dest_name)
                    body = msg[2]
                    session.process_msg(dest_name, message_id, body)
                    break

    def _get_or_create_session(self, session_id, on_message_cb):
        if not self.session_dict.has_key(session_id):
            self.session_dict[session_id] = Session(session_id, on_message_cb)
        return self.session_dict[session_id]

    def _get_or_create_dest(self, dest_name):
        dests = self.dest_dict
        if not dests.has_key(dest_name):
            dests[dest_name] = FileQueue(dest_name,
                                      dir=self.dir,
                                      fsync_millis=self.fsync_millis)
            self.dest_subscribers[dest_name] = deque()
        return dests[dest_name]

class FileQueue(object):

    def __init__(self, name, dir=None, ack_timeout=120, fsync_millis=0,
                 compress_files=False):
        self.compress_files = compress_files
        self._validate_name(name)
        dir = dir or os.getcwd()
        self.dir = dir
        self.name = name
        self.ack_timeout_millis = int(ack_timeout) * 1000
        self.pending_prune_threshold = 10 * 1024 * 1024
        # dict tracking messages in use
        #   key: uuid of message
        #   value: starting byte offset in "-in-use.dat" file
        self.msgs_in_use = { }
        self.in_use_file_msg_count = 0
        self.pending_message_count = 0
        #
        # pending_file_pos is a byte offset into the
        # pending file. we increment this as we read
        # if we hit EOF then we have an empty queue and
        # can reset the queue file
        self.pending_file_pos = 4
        #
        # each queue has a configurable fsync interval
        # fsync_seconds==0 means we'll fsync on all writes
        self.fsync_seconds = int(fsync_millis) / 1000.0
        self.fsync_pending = 0
        self.fsync_in_use  = 0
        #
        # two files per queue.  one stores pending msgs (not yet consumed)
        # the other stores in use msgs (consumed, but not acked)
        basename = base64.urlsafe_b64encode(name)
        self.pending_filename = os.path.join(dir, "%s.pending.dat" % basename)
        self.in_use_filename  = os.path.join(dir, "%s.in-use.dat" % basename)
        self._load_or_init_state()

    def compress(self, body):
        if self.compress_files:
            return zlib.compress(body)
        else:
            return body

    def decompress(self, body):
        if self.compress_files:
            return zlib.decompress(body)
        else:
            return body

    def pending_messages(self):
        return self.pending_message_count

    def in_use_messages(self):
        return len(self.msgs_in_use)

    def msg_in_use(self, id):
        return self.msgs_in_use.has_key(id.hex)

    def send(self, body, old_id=None):
        id = old_id or uuid.uuid4()
        body = self.compress(body)
        f = open(self.pending_filename, "a")
        f.write(struct.pack("i", len(body)))
        f.write(id.bytes)
        f.write(body)
        self._fsync_pending(f)
        f.close()
        self.pending_message_count += 1
        return id

    def receive(self, auto_ack):
        if self.pending_message_count > 0:
            msg = self._read_pending()
            if not auto_ack:
                self._save_in_use(msg)
            return msg
        else:
            return None

    def ack(self, id):
        if self.msgs_in_use.has_key(id.hex):
            seek_offset = self.msgs_in_use[id.hex]
            del(self.msgs_in_use[id.hex])
            in_use = len(self.msgs_in_use)
            if in_use == 0:
                # last pending message. reset file
                f = open(self.in_use_filename, "w")
                self._fsync_in_use(f)
                f.close()
                self.in_use_file_msg_count = 0
            else:
                # zero out the timeout, marking this message acked
                f = open(self.in_use_filename, "r+")
                f.seek(seek_offset, 1)
                f.write(struct.pack("q", 0))
                self._fsync_in_use(f)
                f.close()
                if (in_use/(self.in_use_file_msg_count*1.0)) < .1:
                    self._rewrite_in_use_file()
                    
        #print "in use: %s" % str(self.msgs_in_use)

    def _read_pending(self):
        f = open(self.pending_filename, "r+")
        f.seek(self.pending_file_pos)
        msg_length = struct.unpack("i", f.read(4))[0]
        id = uuid.UUID(bytes=f.read(16))
        body = self.decompress(f.read(msg_length))
        self.pending_file_pos += msg_length + 20

        pending_fsize = os.path.getsize(self.pending_filename)

        if self.pending_file_pos >= pending_fsize:
            # we've read all pending messages.  we can truncate file
            self.pending_file_pos = 4
            f.seek(0,0)
            f.write(struct.pack("i", self.pending_file_pos))
            f.truncate()
        elif pending_fsize > self.pending_prune_threshold and \
                 self.pending_file_pos > (pending_fsize/2):
            # reclaim disk space used by processed msgs
            f = self._rewrite_pending_file(f, pending_fsize)
        else:
            # still have msgs to read.  write pointer
            f.seek(0,0)
            f.write(struct.pack("i", self.pending_file_pos))
        self._fsync_pending(f)
        f.close()

        self.pending_message_count -= 1
        
        return (self, id, body)

    def _rewrite_in_use_file(self):
        (num, tmp_path) = tempfile.mkstemp(dir=self.dir)
        fsize    = os.path.getsize(self.in_use_filename)
        old_file = open(self.in_use_filename, "r")
        tmp_file = open(tmp_path, "w")
        self.in_use_file_msg_count = 0
        self.msgs_in_use = { }
        tmp_pos = 0
        pos = 0
        requeue_count = 0
        remove_count = 0
        now_millis = int(time.time() * 1000)
        while pos < fsize:
            timeout_raw = old_file.read(8)
            msg_len_raw = old_file.read(4)
            timeout = struct.unpack("q", timeout_raw)[0]
            msg_len = struct.unpack("i", msg_len_raw)[0]
            id = old_file.read(16)
            if timeout > now_millis:
                # in use, not timed out. write to new file
                tmp_file.write(timeout_raw)
                tmp_file.write(msg_len_raw)
                tmp_file.write(id)
                tmp_file.write(old_file.read(msg_len))
                id = uuid.UUID(bytes=id)
                self.msgs_in_use[id.hex] = tmp_pos
                self.in_use_file_msg_count += 1
                tmp_pos += msg_len + 28
            elif timeout > 0:
                # timed out, but not acked. requeue
                self.send(self.decompress(old_file.read(msg_len)),
                          old_id=uuid.UUID(bytes=id))
                requeue_count += 1
            else:
                # acked. skip
                old_file.seek(msg_len, 1)
                remove_count += 1
            pos += msg_len + 28
        old_file.close()
        tmp_file.close()
        os.rename(tmp_path, self.in_use_filename)
        #print "rewrite in_use. old_size=%d new_size=%d - kept=%d  " + \
        #    "requeued=%d  removed=%d" % \
        #                 (fsize, os.path.getsize(self.in_use_filename),
        #                  self.in_use_file_msg_count, requeue_count,
        #                  remove_count)

    def _rewrite_pending_file(self, f, pending_fsize):
        (num, tmp_path) = tempfile.mkstemp(dir=self.dir)
        i = self.pending_file_pos
        expected_size = pending_fsize - i
        #print "writing %d bytes to: %s" % (expected_size, tmp_path)
        tmp = open(tmp_path, "w")
        while i < pending_fsize:
            to_read = min(4096, (pending_fsize-i))
            tmp.write(f.read(to_read))
            i += to_read
        tmp.close()
        if os.path.getsize(tmp_path) == expected_size:
            f.close()
            os.rename(tmp_path, self.pending_filename)
            f = open(self.pending_filename, "r+")
            self.pending_file_pos = 4
            f.write(struct.pack("i", self.pending_file_pos))
        else:
            print "ERROR: actual/expected: %d / %d" % \
                  (os.path.getsize(tmp_path), expected_size)
        return f

    def _save_in_use(self, msg):
        id   = msg[1]
        body = self.compress(msg[2])
        timeout = int(time.time() * 1000) + self.ack_timeout_millis
        offset = 0
        if os.path.exists(self.in_use_filename):
            offset = os.path.getsize(self.in_use_filename)
        f = open(self.in_use_filename, "a")
        f.write(struct.pack("q", timeout))
        f.write(struct.pack("i", len(body)))
        f.write(id.bytes)
        f.write(body)
        self._fsync_in_use(f)
        f.close()
        self.msgs_in_use[id.hex] = offset
        self.in_use_file_msg_count += 1
        #print "_save_in_use size: %d" % os.path.getsize(self.in_use_filename)

    def _load_or_init_state(self):
        if os.path.exists(self.pending_filename):
            self._load_pending_state()
            self._load_in_use_state()
        else:
            f = open(self.pending_filename, "a")
            f.write(struct.pack("i", self.pending_file_pos))
            f.close()

    def _load_pending_state(self):
        finfo = os.stat(self.pending_filename)
        fsize = finfo.st_size
        f = open(self.pending_filename, "r")
        self.pending_file_pos = struct.unpack("i", f.read(4))[0]
        f.seek(self.pending_file_pos)
        pos = self.pending_file_pos
        self.pending_message_count = 0
        while pos < fsize:
            msg_len = struct.unpack("i", f.read(4))[0] + 16
            f.seek(msg_len, 1)
            pos += msg_len + 4
            self.pending_message_count += 1
        f.close()

    def _load_in_use_state(self):
        self.msgs_in_use.clear()
        self.in_use_file_msg_count = 0
        if os.path.exists(self.in_use_filename):
            finfo = os.stat(self.in_use_filename)
            fsize = finfo.st_size
            f = open(self.in_use_filename, "r")
            pos = 0
            while pos < fsize:
                timeout = struct.unpack("q", f.read(8))[0]
                msg_len = struct.unpack("i", f.read(4))[0]
                id = uuid.UUID(bytes=f.read(16))
                f.seek(msg_len, 1)
                if timeout > 0:
                    self.msgs_in_use[id.hex] = pos            
                pos += msg_len + 28
                self.in_use_file_msg_count += 1
            f.close()

    def _destroy(self):
        self._delete_file(self.pending_filename)
        self._delete_file(self.in_use_filename)

    def _delete_file(self, filename):
        if os.path.exists(filename):
            os.remove(filename)

    def _fsync_pending(self, f):
        if time.time() > (self.fsync_pending + self.fsync_seconds):
            os.fsync(f.fileno())
            self.fsync_pending = time.time()

    def _fsync_in_use(self, f):
        if time.time() > (self.fsync_in_use + self.fsync_seconds):
            os.fsync(f.fileno())
            self.fsync_in_use = time.time()

    def _validate_name(self, name):
        if not type(name) is types.StringType:
            raise ValueError("Queue name must be a string")
        name_regex = re.compile("^[a-zA-Z0-9\/\-\.\_]+$")
        if not name_regex.match(name):
            raise ValueError("Invalid queue name: %s" % name)
