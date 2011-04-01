
# radiator #

A queue your operations team will actually deploy 

## why I wrote it ##

The infrastructure your system provides will impact the design of your application.

SQL databases aren't always the best choice for persistence, but everyone knows how 
to run them, monitor them, and replicate them.

Message queues have never reached that level of ubiquity or familiarity.  Consequently
application developers end up writing their own queues -- usually on top of the SQL 
database they have available.  These queues usually suck.  I know, because I've written 
about a dozen of them myself.

Having an async task queue enables all sorts of very good design strategies.  It gets
especially tasty when you layer synchronous semantics on top if it, which I hope to do very soon.

## project goals ##

If you're like me...

- You want the same confidence in your message broker that you have in your database
- You can't afford to lose messages if you define a persistent queue
- You want good performance on modest hardware (~1000 persistent msgs/second)
- You want support for persistent and memory-only queues
- You want good documentation
- You like simplicity, and want to be able to read the source and understand what it does

beanstalkd is **almost** my perfect queue.  The main thing it lacks is support for topics and blending
memory-only and persistent queues.  It's also written in C, so I can barely compile it, let alone hack on
it.  

radiator aims to address the above goals.  Basically I want to be about 60% as fast as beanstalkd with 
very high stability and some extra features.  And I want it written in Python because I lurve it.

## features ##

Message types:

You configure your queues and topics in a single config file.  Optionally
clients may create queues / topics at runtime if you allow this.

- Traditional queues (each message consumed by a single worker)
  - Persistent or memory-only
  - Use case: async worker tasks
- Fanout queues (messages are replicated to 1 or more child queues)
  - Can be used to implement a persistent form of publish/subscribe topics
   - Use case: event notification with potentially offline subscribers
- Topics
   - Memory-only
   - All connected subscribers get all messages sent to the topic
   - Use case: real-time applications that can afford to lose messages if not connected (e.g. chat)

Design:

- Single threaded.  gevent is used to manage client connections.
- Memory queues are trivial, and have a configurable limit on max size
- Persistent queues are file backed.  No messages are stored in memory.
  - fsync frequency is configurable (good idea beanstalkd!)

## protocols and clients ##

radiator uses the [Stomp protocol](http://stomp.codehaus.org/Protocol) with a few limitations:

- No authentication support (CONNECT ignores username/password)
- No transactions (BEGIN, COMMIT, ABORT all no-op currently)
- SUBSCRIBE does not support selectors

[Stomp clients](http://stomp.codehaus.org/Clients) are available for many languages.  
Our goal is to work with any existing client, provided it complies with the spec.

## message flow control ##

The Stomp spec doesn't specify how a client indicates 
whether it is ready to consume another message.  Clients subscribe to queues, and then the
server begins sending messages.  It's left to the broker to decide how to implement flow control.

What happens if you have a queue with 10,000 messages in it and a new client connects and
subscribes to the queue?  Should the server start sending all the messages to that client?  Probably not.

radiator implements flow control in the following manner:

- A single client will only be sent a single message across all subscribed **queues** until it ACKs the message
   - Subscribe with ack: auto can be **very** dangerous.  You will get all the messages continuously
- A single client will continuously receive all messages sent to subscribed **topics** with no flow control

For example:

- Client C1 connects and subscribes to queues Q1 and Q2, and topic T1
- Producer sends M1 and M2 to Q1
- radiator sends M1 to C1
- C1 processes message
- Producer sends M3 to Q2
- C1 acks M1
- radiator sends M3 to C1 (clients round-robin through subscribed queues)
- Producer sends M4 to T1
- radiator sends M4 to C1
- C1 acks M3
- radiator sends M2 to C1
- C1 acks M2

A few suggestions based on this:

- **Always** send **ack: client** when subscribing to queues.  We support ack: auto to be compliant, but for queues this seems like a terrible idea.
- If you need higher concurrency when suscribing to queues, make another connection
  - Connections are cheap in radiator given the gevent I/O model
- Be careful with multiple subscriptions on a single connection.
- Be careful with topics in general.  You can really get firehosed with data depending on the producer workload.

## roadmap ##

These are not supported yet, but are things we wish to support soon (6-12 months):

- Return queues with message correlation (to enable synchronous call semantics)
- Celery support
- Stomp transaction support
- Stomp authentication support (plugable)
- Potentially beanstalkd protocol support
  - No priority or delay support
  - No bury/kick support

## non-goals ##

These are things we don't plan on supporting soon:

- priority queueing 
- delayed execution
- JMX style message selectors
- AMQP support (maybe someday, but it's a huge spec)

We think these are all very useful features, but I'm not smart enough to gracefully implement
them without jeopardizing the primary goals of durability, performance, and simplicity for the
currently supported use cases.
