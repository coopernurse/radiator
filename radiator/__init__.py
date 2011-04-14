
import logging
import uuid
import struct
import os
import time
import re
import types
import base64
import random
import tempfile

logger = logging.getLogger('radiator')

def start_server(reactor,
                 dir=None,
                 fsync_millis=0,
                 rewrite_interval_secs=300):
    broker = Broker(dir=dir, fsync_millis=fsync_millis,
                    rewrite_interval_secs=rewrite_interval_secs)
    reactor.start_server(broker, blocking=True)

def now_millis():
    return int(time.time() * 1000)

class RadiatorTimeout(Exception):

    pass

class Subscription(object):

    def __init__(self, dest_name, auto_ack, dest, wildcard_add=False):
        self.dest_name = dest_name
        self.auto_ack  = auto_ack
        self.dest = dest
        self.wildcard_add = wildcard_add

    def matches(self, dest_name):
        if self.dest_name.endswith(".>"):
            s = self.dest_name[:dest_name.rfind(".>")]
            return dest_name.startswith(s)

class Session(object):

    def __init__(self, session_id, send_message_cb):
        self.session_id       = session_id
        self.send_message_cb  = send_message_cb
        self.subscriptions = { }
        self.busy = False

    def destroy(self):
        for dest_name, sub in self.subscriptions.items():
            if sub.dest:
                sub.dest.unsubscribe(self)
        self.subscriptions.clear()

    def subscribe(self, dest, auto_ack, wildcard_add=False):
        if not self.subscriptions.has_key(dest.name):
            sub = Subscription(dest.name, auto_ack, dest, wildcard_add)
            self.subscriptions[dest.name] = sub
            if dest:
                dest.subscribe(self)
                self.pull_message(dest)

    def unsubscribe(self, dest):
        if self.subscriptions.has_key(dest.name):
            dest.unsubscribe(self)
            del(self.subscriptions[dest.name])

    def on_dest_created(self, dest):
        if not self.subscriptions.has_key(dest.name):
            parent_sub = None
            for sub in self.subscriptions.values():
                if sub.matches(dest.name):
                    parent_sub = sub
                    break
            if parent_sub:
                self.subscribe(dest, parent_sub.auto_ack, True)

    def pull_message(self, dest=None):
        msg_sent = False
        if not self.busy:
            msg = None
            auto_ack = True
            if dest:
                auto_ack = self.subscriptions[dest.name].auto_ack
                msg = dest.receive(auto_ack)
            else:
                my_dests = self.subscriptions.values()
                random.shuffle(my_dests)
                for d in my_dests:
                    msg = d.dest.receive(d.auto_ack)
                    if msg:
                        break
            if msg:
                self.busy = (not auto_ack)
                msg_sent  = True
                dest_name = msg[0].name
                msg_id    = msg[1].id.hex+","+msg[0].name
                body      = msg[1].body
                self.send_message_cb(dest_name, msg_id, body)
        return msg_sent

    def send_message(self, dest_name, msg_id, body):
        self.send_message_cb(dest_name, msg_id, body)

class Broker(object):

    def __init__(self, dir=None,
                 fsync_millis=0,
                 rewrite_interval_secs=300):
        self.dir = dir
        self.fsync_millis = fsync_millis
        self.rewrite_interval_secs = rewrite_interval_secs
        #
        #   key: dest_name
        # value: Dest obj (provides send(), receive(), ack())
        self.dest_dict        = { }
        #
        #   key: session_id
        # value: Session obj
        self.session_dict     = { }

    def destroy_session(self, session_id):
        if self.session_dict.has_key(session_id):
            self.session_dict[session_id].destroy()
            del self.session_dict[session_id]

    def send(self, dest_name, body):
        self._get_or_create_dest(dest_name).send(body)

    def subscribe(self, dest_name, auto_ack, session_id, on_message_cb):
        session = self._get_or_create_session(session_id, on_message_cb)
        dest    = self._get_or_create_dest(dest_name)
        session.subscribe(dest, auto_ack)

    def unsubscribe(self, dest_name, session_id):
        if self.session_dict.has_key(session_id):
            session = self.session_dict[session_id]
            dest    = self._get_or_create_dest(dest_name)
            session.unsubscribe(dest)

    def ack(self, session_id, message_id):
        (message_id, dest_name) = message_id.split(",")
        self._get_or_create_dest(dest_name).ack(uuid.UUID(message_id))
        if self.session_dict.has_key(session_id):
            session = self.session_dict[session_id]
            session.busy = False
            session.pull_message()

    def _get_or_create_session(self, session_id, on_message_cb):
        if not self.session_dict.has_key(session_id):
            self.session_dict[session_id] = Session(session_id, on_message_cb)
        return self.session_dict[session_id]

    def _get_or_create_dest(self, dest_name):
        dest = None
        dests = self.dest_dict
        if dests.has_key(dest_name):
            dest = dests[dest_name]
        else: 
            if dest_name.find("/topic/") == 0:
                dest = PubSubTopic(dest_name)
            else:
                rw_secs = self.rewrite_interval_secs
                dest = FileQueue(dest_name,
                                 dir=self.dir,
                                 rewrite_interval_secs=rw_secs,
                                 fsync_millis=self.fsync_millis)
            dests[dest_name] = dest
            for session in self.session_dict.values():
                session.on_dest_created(dest)
        return dest

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
        return "MessageHeader pos=%s id=%s" % (self.pos, self.id.hex)

class BaseDestination(object):

    def __init__(self, name):
        #self._validate_name(name)
        self.name = name
        self.subscribers = { }

    def subscribe(self, session):
        if not self.subscribers.has_key(session.session_id):
            self.subscribers[session.session_id] = session

    def unsubscribe(self, session):
        if self.subscribers.has_key(session.session_id):
            del(self.subscribers[session.session_id])
        
    def send(self, body):
        raise NotImplementedError

    def receive(self, auto_ack):
        raise NotImplementedError

    def ack(self, id):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def destroy(self):
        for s_id, session in self.subscribers.items():
            session.unsubscribe(self)
        self.subscribers.clear()

    def _validate_name(self, name):
        if not type(name) is types.StringType:
            raise ValueError("Queue name must be a string")
        name_regex = re.compile("^[a-zA-Z0-9\/\-\.\_]+$")
        if not name_regex.match(name):
            raise ValueError("Invalid queue name: %s" % name)

class PubSubTopic(BaseDestination):

    def __init__(self, name):
        BaseDestination.__init__(self, name)
        self.messages = [ ]

    def send(self, body):
        id = uuid.uuid4()
        for k, v in self.subscribers.items():
            v.send_message(self.name, id.hex, body)

    def receive(self, auto_ack):
        return None

    def ack(self, id):
        pass

    def close():
        pass

class FileQueue(BaseDestination):

    def __init__(self, name, dir=None, ack_timeout=120, fsync_millis=0,
                 rewrite_interval_secs=300,
                 compress_files=False):
        BaseDestination.__init__(self, name)
        self.version = 1
        self.compress_files = compress_files
        dir = dir or os.getcwd()
        self.dir = dir
        self.rewrite_interval_secs = rewrite_interval_secs
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
        self.f = None
        self._load_or_init_state()

    def pending_messages(self):
        return self.pending_message_count

    def in_use_messages(self):
        return len(self.msgs_in_use)

    def msg_in_use(self, id):
        return self.msgs_in_use.has_key(id.hex)

    def close(self):
        self.f.close()
        self.f = None

    def destroy(self):
        BaseDestination.destroy(self)
        self._delete_file(self.filename)

    def send(self, body):
        self._open()
        id = uuid.uuid4()
        self.f.seek(0, 2) # go to end of file
        msg_header = MessageHeader(self.f.tell(),
                                   now_millis(),
                                   0, 0, id, 0, 0)
        msg_header.write(self.f, "", body)
        self._fsync()
        self.pending_message_count += 1
        self.total_messages += 1
        self._dump("send %s" % id.hex)
        for k,v in self.subscribers.items():
            if v.pull_message(self):
                break
        return id

    def receive(self, auto_ack):
        self._open()
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
            self._dump("receive %s" % msg.id.hex)
            if auto_ack:
                self.ack(msg.id)
            return (self, msg)
        else:
            return None

    def ack(self, id):
        self._open()
        if self.msgs_in_use.has_key(id.hex):
            msg_header = self.msgs_in_use[id.hex]
            del(self.msgs_in_use[id.hex])
            # zero out the timeout, marking this message acked
            self.f.seek(msg_header.pos + 16)
            self.f.write(struct.pack("q", 0))
            self._fsync()
            active = self.pending_message_count + len(self.msgs_in_use)
            self._dump("ack before rewrite %s" % id.hex)
            active_pct = active / (self.total_messages * 1.0)
            if active_pct < 0.1 and self.next_rewrite < time.time():
                self._rewrite_file()
        else:
            logger.error("ack: %s: no msg in use with id: %s" % \
                         (self.name, id.hex))
        self._dump("ack %s" % id.hex)

    ##################################################################

    def _rewrite_file(self):
        start = time.time()
        self.next_rewrite = time.time() + self.rewrite_interval_secs
        (tmp_fd, tmp_path) = tempfile.mkstemp(dir=self.dir)
        tmp_file = os.fdopen(tmp_fd, "w")
        tmp_file.write(struct.pack("q", 12))
        tmp_file.write(struct.pack("i", self.version))
        fsize    = os.path.getsize(self.filename)
        self.msgs_in_use.clear()
        self.pending_message_count = 0
        pos = 12
        self.pending_file_pos = pos
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
                if self.pending_message_count == 0:
                    # position of first pending msg in new file
                    self.pending_file_pos = tmp_file.tell()
                self.pending_message_count += 1                    

            if write_msg:
                msg_header.copy(self.f, tmp_file)
            else:
                self.f.seek(msg_header.header_size,1)
                self.f.seek(msg_header.body_size,1)

        # add ack expired messages to end of queue
        for msg_header in to_requeue:
            msg_header.dequeue_time = 0
            msg_header.ack_timeout  = 0
            if self.pending_message_count == 0:
                # position of first pending msg in new file
                self.pending_file_pos = tmp_file.tell()
            msg_header.copy(self.f, tmp_file)
            self.pending_message_count += 1

        self.total_messages = self.pending_message_count+len(self.msgs_in_use)
        self.f.close()
        tmp_file.seek(0,0)
        tmp_file.write(struct.pack("q", self.pending_file_pos))
        tmp_file.close()
        os.rename(tmp_path, self.filename)
        self.f = open(self.filename, "r+")
        self.f.write(struct.pack("q", self.pending_file_pos))
        self.f.write(struct.pack("i", self.version))
        self._fsync(True)
        #elapsed = int((time.time() - start) * 1000)
        #print "_rewrite_file. elapsed=%d old_size=%d new_size=%d - kept=%d requeued=%d  removed=%d" %  (elapsed, fsize, os.path.getsize(self.filename), self.total_messages, len(to_requeue), remove_count)
        self._dump("_rewrite_file")

    def _open(self):
        if not self.f:
            self._load_or_init_state()

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
        self.total_messages = self.pending_message_count+len(self.msgs_in_use)
        self._dump("init")

    def _load_state(self):
        finfo = os.stat(self.filename)
        fsize = finfo.st_size
        self.f = open(self.filename, "r+")
        self.pending_file_pos = struct.unpack("q", self.f.read(8))[0]
        self.version = struct.unpack("i", self.f.read(4))[0]
        pos = 12
        while pos < fsize:
            self._dump("_load_state")
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

    def _delete_file(self, filename):
        if os.path.exists(filename):
            os.remove(filename)

    def _fsync(self, force=False):
        self.f.flush()
        if force or (time.time() > (self.last_fsync + self.fsync_seconds)):
            os.fsync(self.f.fileno())
            self.last_fsync = time.time()

    def _dump(self, msg):
        #print "%s - pos=%d pending=%d in_use=%d" % (msg, self.pending_file_pos, self.pending_message_count, len(self.msgs_in_use))
        pass

