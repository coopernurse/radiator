import uuid
import time

from radiator import RadiatorTimeout

def dict_get(d, key, default_val):
    if d.has_key(key):
        return d[key]
    else:
        return default_val

class BaseStompConnection(object):

    def drain(self, max=0, timeout=-1):
        i = 0
        self.f.timeout = timeout
        while self.connected and (max == 0 or i < max):
            try:
                self._dispatch(self._read_frame(timeout))
                i += 1
            except BufferError:
                # client disconnected
                #print "client disconnected"
                self.connected = False
                break
            except RadiatorTimeout:
                # ok
                break
        return i

    def _write_frame(self, command, headers=None, body=None):
        #print "SEND: command=%s headers=%s body=%s" % \
        #      (command, str(headers), str(body))
        if body:
            headers.append("content-length:%d" % len(body))        
        f = self.f
        f.write(command)
        f.write("\n")
        if headers:
            for h in headers:
                f.write(h)
                f.write("\n")
        f.write("\n")
        if body:
            f.write(body)
        f.write(chr(0))
        f.flush()

    def _read_frame(self, timeout=-1):
        self.timeout = timeout
        
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
            frame["body"] = "".join(body).rstrip("\n").rstrip("\r")

        #print "RECV: command=%s headers=%s body=%s" % \
        #      (frame["command"], frame["headers"], frame["body"])
        return frame

    def _readline(self):
        line = self.f.readline()
        if line:
            return line
        else:
            raise RadiatorTimeout
    

def on_error_default(err_message, body):
    print "STOMP error: %s %s" % (err_message, str(body))

class StompClient(BaseStompConnection):

    def __init__(self, conn, on_error=None, write_timeout=60):
        self.f = conn
        self.write_timeout = write_timeout
        self.on_error = on_error or on_error_default
        self.callbacks = { }
        self.receipts  = [ ]
        self.connect()

    def connect(self):
        self.connected = True
        self._write_frame("CONNECT")
        f = self._read_frame(100)
        if f["command"] == "CONNECTED":
            self.session_id = f["headers"]["session"]
        else:
            raise IOError("Invalid frame after CONNECT: %s" % str(f))

    def disconnect(self):
        self._write_frame("DISCONNECT")
        self.f.close()
        self.connected = False
        self.f = None

    def call(self, dest_name, body, timeout=60, call_sleep_sec=0.005,
             headers=None, receipt=False):
        resp_body = [ ]
        def on_msg(s, m_id, r_body):
            resp_body.append(r_body)
            
        reply_to_id = uuid.uuid4().hex
        if not headers:
            headers = { }
        headers['reply-to'] = reply_to_id
        self.callbacks[reply_to_id] = on_msg
        self.send(dest_name, body, headers, receipt)
        timeout_sec = time.time() + timeout
        while time.time() < timeout_sec and len(resp_body) == 0:
            time.sleep(self.call_sleep_sec)
        if resp_body:
            return resp_body[0]
        else:
            raise RadiatorTimeout("No response to message: %s within timeout: %.1f" % \
                                  (body, timeout))

    def send(self, dest_name, body, headers=None, receipt=False):
        headers_arr = [ "destination:%s" % dest_name ]
        if headers:
            for k,v in headers.items():
                headers_arr.append("%s:%s" % (k, v))
        r = None
        if receipt:
            r = self._create_receipt(headers)
        self._write_frame("SEND", headers=headers_arr, body=body)
        if receipt: self._wait_for_receipt(r)

    def subscribe(self, dest_name, callback, auto_ack=True, receipt=False):
        ack = "client"
        if auto_ack: ack = "auto"
        headers = [ "destination:%s" % dest_name, "ack:%s" % ack ]
        headers = self._create_headers(receipt, headers)
        self._write_frame("SUBSCRIBE", headers=headers)
        if receipt: self._wait_for_receipt(headers['receipt'])
        self.callbacks[dest_name] = callback

    def unsubscribe(self, dest_name, receipt=False):
        headers = self._create_headers(receipt, ["destination:%s" % dest_name])
        self._write_frame("UNSUBSCRIBE", headers=headers)
        if receipt: self._wait_for_receipt(headers['receipt'])
        self.drain(timeout=0.1)
        if self.callbacks.has_key(dest_name):
            del(self.callbacks[dest_name])

    def ack(self, msg_id, receipt=False):
        headers = self._create_headers(receipt, ["message-id:%s" % msg_id])
        self._write_frame("ACK", headers=headers)
        if receipt: self._wait_for_receipt(headers['receipt'])

    def _dispatch(self, frame):
        headers = frame["headers"]
        cmd = frame["command"]
        if   cmd == "MESSAGE"  : self._on_message(frame)
        elif cmd == "RECEIPT"  : self.receipts.append((headers["receipt"]))
        elif cmd == "ERROR"    : self.on_error(headers["message"],
                                               dict_get(frame, "body", ""))
        else:
            print "Unknown command: %s" % cmd

    def _on_message(self, frame):
        dest_name  = frame["headers"]["destination"]
        message_id = frame["headers"]["message-id"]
        body = dict_get(frame, "body", "")
        if self.callbacks.has_key(dest_name):
            self.callbacks[dest_name](self, message_id, body)
        else:
            self.on_error("No subscriber registered for destination: " +
                          "%s - but got message: %s %s" %
                          (dest_name, message_id, body))
            
    def _create_headers(self, receipt, headers):
        if receipt:
            headers.append("receipt:%s" % uuid.uuid4().hex)
        return headers

    def _create_receipt(self, headers):
        receipt = uuid.uuid4().hex
        headers.append("receipt:%s" % receipt)
        return receipt

    def _wait_for_receipt(self, receipt, timeout_sec=60):
        timeout = time.time() + timeout_sec
        while time.time() > timeout:
            self.drain(timeout=.05)
            if receipt in self.receipts:
                self.receipts.remove(receipt)
                return
        raise IOError("No receipt %s received after %d seconds" %
                      (receipt, timeout_sec))

class StompServer(BaseStompConnection):

    def __init__(self, conn, broker):
        self.f = conn
        self.broker = broker
        self.connected = True

    def drain(self):
        # read all messages from this client
        BaseStompConnection.drain(self)
        # connection ended - notify broker to remove
        self.broker.destroy_session(self.session_id)

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
        self._write_frame("CONNECTED",
                          headers=[ "session:%s" % self.session_id.hex ])

    def _send(self, frame):
        self.broker.send(frame["headers"]["destination"], frame["body"])
        self._send_receipt(frame)

    def _subscribe(self, frame):
        cb = lambda dest_name, msg_id, body: self._on_message(dest_name,
                                                              msg_id,
                                                              body)
        auto_ack = not dict_get(frame["headers"], "ack", "") == "client"
        self.broker.subscribe(frame["headers"]["destination"],
                              auto_ack,
                              self.session_id,
                              cb)
        self._send_receipt(frame)

    def _unsubscribe(self, frame):
        self.broker.unsubscribe(frame["headers"]["destination"],
                                self.session_id)
        self._send_receipt(frame)

    def _ack(self, frame):
        self.broker.ack(self.session_id, frame["headers"]["message-id"])
        self._send_receipt(frame)
        
    def _disconnect(self, frame):
        self.connected = False

    def _on_message(self, dest_name, message_id, body):
        #print "_on_message: %s %s %s" % (dest_name, message_id, body)
        self._write_frame("MESSAGE", body=body,
                          headers=["destination:%s" % dest_name,
                                   "message-id:%s" % message_id])

    def _send_receipt(self, frame):
        fh = frame["headers"]
        if fh.has_key("receipt"):
            self._write_frame("RECEIPT",
                              headers=["receipt:%s" % fh["receipt"]])

