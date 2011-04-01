
import uuid
import struct
import os
import time
import re
import types

class FileQueue(object):

    def __init__(self, name, dir=None, ack_timeout=120, fsync_millis=0):
        self._validate_name(name)
        dir = dir or os.getcwd()
        self.name = name
        self.listeners = [ ]
        self.ack_timeout_millis = int(ack_timeout) * 1000
        self.msgs_in_use = { }
        self.pending_message_count = 0
        #
        # pending_file_pos is a byte offset into the
        # pending file. we increment this as we read
        # if we hit EOF then we have an empty queue and
        # can reset the queue file
        self.pending_file_pos      = 4
        #
        # each queue has a configurable fsync interval
        # fsync_seconds==0 means we'll fsync on all writes
        self.fsync_seconds = int(fsync_millis) / 1000.0
        self.fsync_pending = 0
        self.fsync_in_use  = 0
        #
        # two files per queue.  one stores pending msgs (not yet consumed)
        # the other stores in use msgs (consumed, but not acked)
        self.filename        = os.path.join(dir, "%s-pending.dat" % name)
        self.in_use_filename = os.path.join(dir, "%s-in-use.dat" % name)        
        self._load_or_init_state()

    def pending_messages(self):
        return self.pending_message_count

    def in_use_messages(self):
        return len(self.msgs_in_use)

    def msg_in_use(self, id):
        return self.msgs_in_use.has_key(id.bytes)

    def send(self, msg):
        id = uuid.uuid4()
        f = open(self.filename, "a")
        f.write(struct.pack("i", len(msg)))
        f.write(id.bytes)
        f.write(msg)
        self._fsync_pending(f)
        f.close()
        self.pending_message_count += 1
        self._dequeue()
        return id

    def add_listener(self, listener, auto_ack=True):
        self.listeners.append((listener, auto_ack))
        self._dequeue()

    def ack(self, id):
        if self.msgs_in_use.has_key(id.bytes):
            f = open(self.in_use_filename, "r+")
            f.seek(self.msgs_in_use[id.bytes])
            f.write(struct.pack("q", 0))
            self._fsync_in_use(f)
            f.close()
            del(self.msgs_in_use[id.bytes])

    def _dequeue(self):
        if len(self.listeners) > 0 and self.pending_message_count > 0:
            msg = self._read_pending()
            (listener, auto_ack) = self.listeners.pop(0)
            if not auto_ack:
                self._save_in_progress(msg)
            listener(msg)

    def _read_pending(self):
        f = open(self.filename, "r")
        f.seek(self.pending_file_pos)
        msg_length = struct.unpack("i", f.read(4))[0]
        id = uuid.UUID(bytes=f.read(16))
        body = f.read(msg_length)
        self.pending_file_pos += msg_length + 20

        f = open(self.filename, "r+")
        f.write(struct.pack("i", self.pending_file_pos))
        self._fsync_pending(f)
        f.close()

        self.pending_message_count -= 1
        
        return (self, id, body)

    def _save_in_progress(self, msg):
        id   = msg[1]
        body = msg[2]
        timeout = int(time.time() * 1000) + self.ack_timeout_millis
        pos = 0
        if os.path.exists(self.in_use_filename):
            finfo = os.stat(self.in_use_filename)
            pos = finfo.st_size
        f = open(self.in_use_filename, "a")
        f.write(struct.pack("q", timeout))
        f.write(struct.pack("i", len(body)))
        f.write(id.bytes)
        f.write(body)
        self._fsync_in_use(f)
        f.close()
        self.msgs_in_use[id.bytes] = pos

    def _load_or_init_state(self):
        if os.path.exists(self.filename):
            self._load_pending_state()
            self._load_in_use_state()
        else:
            f = open(self.filename, "a")
            f.write(struct.pack("i", self.pending_file_pos))
            f.close()

    def _load_pending_state(self):
        finfo = os.stat(self.filename)
        fsize = finfo.st_size
        f = open(self.filename, "r")
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
        self.msgs_in_use = { }
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
                    self.msgs_in_use[id.bytes] = pos            
                pos += msg_len + 28
            f.close()

    def _destroy(self):
        self._delete_file(self.filename)
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
        name_regex = re.compile("^[a-zA-Z0-9\-\.\_]+$")
        if not name_regex.match(name):
            raise ValueError("Invalid queue name: %s" % name)
