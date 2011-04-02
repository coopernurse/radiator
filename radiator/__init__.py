
import uuid
import struct
import os
import time
import re
import types
import base64

from gevent.pool import Pool
from gevent.server import StreamServer

def start_server(bind_addr, port, pool_size=5000, dir=None, fsync_millis=0):
    broker = Broker(dir=dir, fsync_millis=fsync_millis)
    pool   = Pool(pool_size)
    server = StreamServer((bind_addr, port),
                          lambda socket, addr: Connection(socket.makefile(), broker).start(),
                          spawn=pool)
    server.serve_forever()

def dict_get(d, key, default_val):
    if d.has_key(key):
        return d[key]
    else:
        return default_val

class Session(object):

    def __init__(self, session_id, on_message_cb):
        self.session_id = session_id
        self.on_message_cb = on_message_cb
        self.subscribed_dests = { }
        self.order_of_dequeue = [ ]
        self.busy = False

    def subscribe(self, dest_name, auto_ack):
        if not self.subscribed_dests.has_key(dest_name):
            self.order_of_dequeue.append(dest_name)
        self.subscribed_dests[dest_name] = auto_ack
        self._dump("subscribe %s" % dest_name)

    def unsubscribe(self, dest_name):
        if self.subscribed_dests.has_key(dest_name):
            del(self.subscribed_dests[dest_name])
            self.order_of_dequeue.remove(dest_name)
        self._dump("unsubscribe %s" % dest_name)

    def get_auto_ack(self, dest_name):
        return self.subscribed_dests[dest_name]

    def process_msg(self, dest_name, message_id, body):
        self.busy = True
        self.order_of_dequeue.remove(dest_name)
        self.order_of_dequeue.append(dest_name)
        self.on_message_cb(dest_name, message_id, body)

    def _dump(self, msg):
        print "msg: %s\t%s\t%s" % (msg, str(self.subscribed_dests), str(self.order_of_dequeue))

class Broker(object):

    def __init__(self, dir=None, fsync_millis=0):
        self.dest_dict    = { }
        self.session_dict = { }
        self.dir = dir
        self.fsync_millis = fsync_millis

    def send(self, dest_name, body):
        id = self._get_or_create_dest(dest_name).send(body)
        print "send dest=%s  body=%s  id=%s" % (dest_name, body, id.hex)

    def subscribe(self, dest_name, auto_ack, session_id, on_message_cb):
        session = self._get_or_create_session(session_id, on_message_cb)
        session.subscribe(dest_name, auto_ack)
        self._send_msg_to_session(session)

    def unsubscribe(self, dest_name, session_id):
        if self.session_dict.has_key(session_id):
            self.session_dict[session_id].unsubscribe(dest_name)

    def ack(self, session_id, message_id):
        (message_id, dest_name) = message_id.split(",")
        print "ack session=%s message=%s" % (session_id, message_id)
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
        if not self.dest_dict.has_key(dest_name):
            self.dest_dict[dest_name] = FileQueue(dest_name, dir=self.dir, fsync_millis=self.fsync_millis)
        return self.dest_dict[dest_name]

class Connection(object):

    def __init__(self, fileobj, broker):
        self.f = fileobj
        self.broker = broker
        self.connected = True

    def start(self):
        while self.connected:
            try:
                self._dispatch(self._read_frame())
            except BufferError:
                # client disconnected
                print "client disconnected"
                break

    def _dispatch(self, frame):
        cmd = frame['command']
        if   cmd == "CONNECT"     : self._connect(frame)
        elif cmd == "SEND"        : self._send(frame)
        elif cmd == "DISCONNECT"  : self._disconnect(frame)
        elif cmd == "SUBSCRIBE"   : self._subscribe(frame)
        elif cmd == "UNSUBSCRIBE" : self._unsubscribe(frame)
        elif cmd == "ACK"         : self._ack(frame)
        else:
            print "Unknown command: %s" % cmd

    def _connect(self, frame):
        self.session_id = uuid.uuid4()
        self._write_frame("CONNECTED", headers=[ "session: %s" % self.session_id.hex ])

    def _send(self, frame):
        self.broker.send(frame["headers"]["destination"], frame["body"])
        self._send_receipt(frame)

    def _subscribe(self, frame):
        auto_ack = not dict_get(frame["headers"], "ack", "") == "client"
        self.broker.subscribe(frame["headers"]["destination"],
                              auto_ack,
                              self.session_id,
                              lambda dest_name, message_id, body: self._on_message(dest_name, message_id, body))
        self._send_receipt(frame)

    def _unsubscribe(self, frame):
        self.broker.unsubscribe(frame["headers"]["destination"], self.session_id)
        self._send_receipt(frame)

    def _ack(self, frame):
        self.broker.ack(self.session_id, frame["headers"]["message-id"])
        self._send_receipt(frame)
        
    def _disconnect(self, frame):
        self.connected = False

    def _on_message(self, dest_name, message_id, body):
        print "_on_message: %s %s %s" % (dest_name, message_id, body)
        self._write_frame("MESSAGE",
                          headers=["destination: %s" % dest_name, "message-id: %s" % message_id], body=body)

    def _send_receipt(self, frame):
        if frame["headers"].has_key("receipt-id"):
            self._write_frame("RECEIPT", headers=["receipt-id: %s" % frame["headers"]["receipt-id"]])

    def _write_frame(self, command, headers=None, body=None):
        print "SENDING FRAME: command=%s headers=%s body=%s" % (command, str(headers), str(body))
        self.f.write(command)
        self.f.write("\n")
        if headers:
            for h in headers:
                self.f.write(h)
                self.f.write("\n")
        self.f.write("\n")
        if body:
            self.f.write(body)
            self.f.write("\n")
        self.f.write(chr(0))
        self.f.write("\n")
        self.f.flush()

    def _read_frame(self):
        frame = { "headers" : { } }

        # command is first
        frame["command"] = self._readline().strip()
        
        # read headers
        content_length = 0
        line = self._readline()
        while line.strip() != "":
            pos = line.find(":")
            if pos >= 0:
                key = line[:pos].strip()
                val = line[(pos+1):].strip()
                frame["headers"][key] = val
                if key.lower() == "content-length":
                    content_length = int(val)
            line = self._readline()
                
        # read body
        if content_length > 0:
            frame["body"] = self.f.read(content_length)
            while True:
                if self.f.read(1) == chr(0): break
        else:
            body = [ ]
            c = self.f.read(1)
            while c != chr(0):
                body.append(c)
                c = self.f.read(1)
            frame["body"] = "".join(body)
        
        print "frame=%s" % str(frame)
        return frame

    def _readline(self):
        line = self.f.readline()
        if not line:
            raise BufferError
        else:
            #print "line: %s" % line
            return line


class FileQueue(object):

    def __init__(self, name, dir=None, ack_timeout=120, fsync_millis=0):
        self._validate_name(name)
        dir = dir or os.getcwd()
        self.name = name
        self.ack_timeout_millis = int(ack_timeout) * 1000
        # dict tracking messages in use
        #   key: uuid of message
        #   value: starting byte offset in "-in-use.dat" file
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
        basename = base64.urlsafe_b64encode(name)
        self.pending_filename = os.path.join(dir, "%s.pending.dat" % basename)
        self.in_use_filename  = os.path.join(dir, "%s.in-use.dat" % basename)
        self._load_or_init_state()

    def pending_messages(self):
        return self.pending_message_count

    def in_use_messages(self):
        return len(self.msgs_in_use)

    def msg_in_use(self, id):
        return self.msgs_in_use.has_key(id.hex)

    def send(self, msg):
        id = uuid.uuid4()
        f = open(self.pending_filename, "a")
        f.write(struct.pack("i", len(msg)))
        f.write(id.bytes)
        f.write(msg)
        self._fsync_pending(f)
        f.close()
        self.pending_message_count += 1
        return id

    def receive(self, auto_ack):
        if self.pending_message_count > 0:
            msg = self._read_pending()
            if not auto_ack:
                self._save_in_progress(msg)
            return msg
        else:
            return None

    def ack(self, id):
        if self.msgs_in_use.has_key(id.hex):
            if len(self.msgs_in_use) == 1:
                f = open(self.in_use_filename, "w")
                self._fsync_in_use(f)
                f.close()
            else:
                f = open(self.in_use_filename, "r+")
                f.seek(self.msgs_in_use[id.hex])
                f.write(struct.pack("q", 0))
                self._fsync_in_use(f)
                f.close()
            del(self.msgs_in_use[id.hex])
        #print "in use: %s" % str(self.msgs_in_use)

    def _read_pending(self):
        f = open(self.pending_filename, "r")
        f.seek(self.pending_file_pos)
        msg_length = struct.unpack("i", f.read(4))[0]
        id = uuid.UUID(bytes=f.read(16))
        body = f.read(msg_length)
        self.pending_file_pos += msg_length + 20

        f = open(self.pending_filename, "r+")
        if self.pending_file_pos >= os.path.getsize(self.pending_filename):
            # we've read all pending messages.  we can truncate file
            self.pending_file_pos = 4
            f.write(struct.pack("i", self.pending_file_pos))
            f.truncate()
        else:
            # still have msgs to read.  write pointer
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
        self.msgs_in_use[id.hex] = pos

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
