
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

def now_millis():
    return int(time.time() * 1000)

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
                msg_tuple = dest.receive(session.get_auto_ack(dest_name))
                if msg_tuple:
                    msg = msg_tuple[1]
                    message_id = "%s,%s" % (msg.id.hex, dest_name)
                    session.process_msg(dest_name, message_id, msg.body)
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

class MessageHeader(object):

    def __init__(self, pos, create_time, dequeue_time, ack_timeout,
                 id, header_size, body_size):
        self.pos = pos
        self.create_time  = create_time
        self.dequeue_time = dequeue_time
        self.ack_timeout  = ack_timeout
        self.id = id
        self.header_size = header_size
        self.body_size   = body_size
        self.total_size  = header_size + body_size + 48

    def copy(self, from_file, to_file):
        self._write_header(to_file)
        from_file.seek(self.pos+48)
        if (self.header_size > 0):
            to_file.write(from_file.read(self.header_size))
        if (self.body_size > 0):
            to_file.write(from_file.read(self.body_size))

    def write(self, to_file, header, body):
        self.header_size = len(header)
        self.body_size   = len(body)
        self._write_header(to_file)
        to_file.write(header)
        to_file.write(body)

    def _write_header(self, to_file):
        to_file.write(struct.pack("q", self.create_time))
        to_file.write(struct.pack("q", self.dequeue_time))
        to_file.write(struct.pack("q", self.ack_timeout))
        to_file.write(self.id.bytes)
        to_file.write(struct.pack("i", self.header_size))
        to_file.write(struct.pack("i", self.body_size))

    def __str__(self):
        #return "MessageHeader pos=%d id=%s create_time=%d dequeue_time=%d " + \
        #       "ack_timeout=%d header_size=%d body_size=%d" % \
        #(self.pos, self.id.hex, self.create_time, self.dequeue_time,
        #        self.ack_timeout, self.header_size, self.body_size)
        return "MessageHeader pos=%s id=%s" % \
               (self.pos, self.id.hex)

class FileQueue(object):

    def __init__(self, name, dir=None, ack_timeout=120, fsync_millis=0,
                 rewrite_interval_seconds=300,
                 compress_files=False):
        self.version = 1
        self.compress_files = compress_files
        self._validate_name(name)
        dir = dir or os.getcwd()
        self.dir = dir
        self.name = name
        self.rewrite_interval_seconds = rewrite_interval_seconds
        self.next_rewrite = 0
        self.ack_timeout_millis = int(ack_timeout * 1000)
        self.pending_prune_threshold = 10 * 1024 * 1024
        # dict tracking messages in use
        #   key: uuid of message
        #   value: tuple (starting byte offset in file, timeout)
        self.msgs_in_use = { }
        self.pending_message_count = 0
        self.total_messages = 0
        #
        # pending_file_pos is a byte offset into the
        # pending file. we increment this as we read
        # if we hit EOF then we have an empty queue and
        # can reset the queue file
        self.pending_file_pos = 12
        #
        # each queue has a configurable fsync interval
        # fsync_seconds==0 means we'll fsync on all writes
        self.fsync_seconds = now_millis()
        self.last_fsync = 0
        #
        # one file per queue
        basename = base64.urlsafe_b64encode(name)
        self.filename = os.path.join(dir, "%s.msg.dat" % basename)
        self._load_or_init_state()

    def pending_messages(self):
        return self.pending_message_count

    def in_use_messages(self):
        return len(self.msgs_in_use)

    def msg_in_use(self, id):
        return self.msgs_in_use.has_key(id.hex)

    def send(self, body):
        id = uuid.uuid4()
        self.f.seek(0, 2) # go to end of file
        msg_header = MessageHeader(self.f.tell(),
                                   now_millis(),
                                   0, 0, id, 0, 0)
        msg_header.write(self.f, "", body)
        self._fsync()
        self.pending_message_count += 1
        self.total_messages += 1
        return id

    def receive(self, auto_ack):
        if self.pending_message_count > 0:
            # grab next msg from queue file, w/body
            msg = self._read_msg(self.pending_file_pos, True)
            # advance file pointer and write it
            self.pending_file_pos += msg.total_size
            self.f.seek(0)
            self.f.write(struct.pack("q", self.pending_file_pos))
            # mark message dequeued
            now = now_millis()
            self.f.seek(msg.pos+8)
            self.f.write(struct.pack("q", now)) # dequeue time
            self.f.write(struct.pack("q", now+self.ack_timeout_millis))
            self._fsync()
            self.pending_message_count -= 1
            self.msgs_in_use[msg.id.hex] = msg
            if auto_ack:
                self.ack(msg.id)
            return (self, msg)
        else:
            return None

    def ack(self, id):
        if self.msgs_in_use.has_key(id.hex):
            msg_header = self.msgs_in_use[id.hex]
            del(self.msgs_in_use[id.hex])
            # zero out the timeout, marking this message acked
            self.f.seek(msg_header.pos + 16)
            self.f.write(struct.pack("q", 0))
            self._fsync()
            active = self.pending_message_count + len(self.msgs_in_use)
            if (active / self.total_messages * 1.0) < 0.1 and \
               self.next_rewrite < time.time():
                self._rewrite_file()
        else:
            print "ERROR: no msg in use with id: %s" % id.hex

    def _rewrite_file(self):
        start = time.time()
        self.next_rewrite = time.time() + self.rewrite_interval_seconds
        (tmp_fd, tmp_path) = tempfile.mkstemp(dir=self.dir)
        tmp_file = os.fdopen(tmp_fd, "w")
        tmp_file.write(struct.pack("q", 12))
        tmp_file.write(struct.pack("i", self.version))
        fsize    = os.path.getsize(self.filename)
        self.msgs_in_use.clear()
        self.pending_message_count = 0
        pos = 12
        self.pending_file_pos = 0
        remove_count = 0
        to_requeue = [ ]
        now_ms = now_millis()
        self.f.seek(pos)
        while pos < fsize:
            write_msg = False
            msg_header = self._read_msg(pos, False)
            pos += msg_header.total_size
            if msg_header.dequeue_time > 0:
                # msg has been dequeued
                if msg_header.ack_timeout == 0:
                    # msg dequeued and acked. we don't need to keep it
                    remove_count += 1
                elif msg_header.ack_timeout < now_ms:
                    # ack expired. re-queue
                    to_requeue.append(msg_header)
                else:
                    # ack not expired - but store new file offset
                    write_msg = True
                    msg_header.pos = tmp_file.tell()
                    self.msgs_in_use[msg_header.id.hex] = msg_header
            else:
                write_msg = True
                if self.pending_file_pos == 0:
                    # position of first pending msg in new file
                    self.pending_file_pos = tmp_file.tell()

            if write_msg:
                msg_header.copy(self.f, tmp_file)
                self.pending_message_count += 1
            else:
                self.f.seek(msg_header.header_size,1)
                self.f.seek(msg_header.body_size,1)

        # add ack expired messages to end of queue
        for msg_header in to_requeue:
            msg_header.dequeue_time = 0
            msg_header.ack_timeout  = 0
            msg_header.copy(self.f, tmp_file)
            self.pending_message_count += 1

        self.total_messages = self.pending_message_count + len(self.msgs_in_use)
        self.f.close()
        tmp_file.seek(0,0)
        tmp_file.write(struct.pack("q", self.pending_file_pos))
        tmp_file.close()
        os.rename(tmp_path, self.filename)
        self.f = open(self.filename, "r+")
        self.f.write(struct.pack("q", self.pending_file_pos))
        self.f.write(struct.pack("i", self.version))
        self._fsync(True)
        elapsed = int((time.time() - start) * 1000)
        #print "_rewrite_file. elapsed=%d old_size=%d new_size=%d - kept=%d requeued=%d  removed=%d" %  (elapsed, fsize, os.path.getsize(self.filename), self.total_messages, len(to_requeue), remove_count)

    def _load_or_init_state(self):
        self.pending_message_count = 0
        self.msgs_in_use.clear()
        if os.path.exists(self.filename):
            self._load_state()
        else:
            self.pending_file_pos = 12
            self.f = open(self.filename, "w")
            self.f.write(struct.pack("q", self.pending_file_pos))
            self.f.write(struct.pack("i", self.version))
            self.f.close()
            self.f = open(self.filename, "r+")
        self.total_messages = self.pending_message_count + len(self.msgs_in_use)

    def _load_state(self):
        finfo = os.stat(self.filename)
        fsize = finfo.st_size
        self.f = open(self.filename, "r+")
        self.pending_file_pos = struct.unpack("q", self.f.read(8))[0]
        self.version = struct.unpack("i", self.f.read(4))[0]
        pos = 12
        while pos < fsize:
            msg_header = self._read_msg(pos, False)
            if msg_header.dequeue_time > 0:
                if msg_header.ack_timeout > 0:
                    self.msgs_in_use[msg_header.id.hex] = msg_header
            else:
                self.pending_message_count += 1
            pos += msg_header.total_size

    def _read_msg(self, pos, read_contents):
        self.f.seek(pos, 0)
        msg = MessageHeader(pos=self.f.tell(),
                            create_time =struct.unpack("q", self.f.read(8))[0],
                            dequeue_time=struct.unpack("q", self.f.read(8))[0],
                            ack_timeout =struct.unpack("q", self.f.read(8))[0],
                            id=uuid.UUID(bytes=self.f.read(16)),
                            header_size=struct.unpack("i", self.f.read(4))[0],
                            body_size=struct.unpack("i", self.f.read(4))[0])
        if read_contents:
            if msg.header_size > 0:
                msg.header = self.f.read(msg.header_size)
            if msg.body_size > 0:
                msg.body = self.f.read(msg.body_size)
        return msg

    def _destroy(self):
        self._delete_file(self.filename)

    def _delete_file(self, filename):
        if os.path.exists(filename):
            os.remove(filename)

    def _fsync(self, force=False):
        self.f.flush()
        if force or (time.time() > (self.last_fsync + self.fsync_seconds)):
            os.fsync(self.f.fileno())
            self.last_fsync = time.time()

    def _validate_name(self, name):
        if not type(name) is types.StringType:
            raise ValueError("Queue name must be a string")
        name_regex = re.compile("^[a-zA-Z0-9\/\-\.\_]+$")
        if not name_regex.match(name):
            raise ValueError("Invalid queue name: %s" % name)

