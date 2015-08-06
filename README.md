# Gilmour

Gilmour is a framework for writing micro-services that exchange data over
non-http transports. Currently the supported backend is Redis PubSub.
Redis pubsub channels are used like "routes".
Gilmour started off simply as a non-http alternative to Sinatra, but has grown into a feature rich microservices communication library and framework.

## Patterns

Gilmour enables two patterns of communication:

### The request-response pattern
This is the most common pattern found in microservice architectures. An entity
sends a `request`, and a subscriber processes it and sends a `reply`. See
`examples/fibonacci.rb` as an example.  Gilmour also allows horizontal scaling
by using __exclusion groups__. When there are multiple instances of a subscriber running, only one subscriber from an exclusion group processes that request.
Eg, if you run `container.rb` from the examples multiple times simultaneously, running `echoclient.rb` or `forkechoclient.rb` will produce the same result, irrespective of how many instances of the container are running. (More of containers later).

### The signal-slot pattern
A less common, but very powerful pattern is event driven service design, or the signal-slot pattern (a term borrowed from the Qt library). One service emits or signals events, which are picked up by one or more subscribers and processed. None of the subscribers send a reply, because a reply really has no meaning in this context. See `examples/signal_slot.rb`.

Which pattern to chose, is primarily dependent on the ownership of failure. If the subscribers own the failures they encounter, then you should consider the signal-slot pattern.

It is also possible to have slots as well as reply subscribers for a given message. Gilmour provides a utility `broadcast` which internally does a signal and a request.

## Composition

Microservices increase the granularity of our services oriented architectures. Borrowing from unix philosohy, they should do one thing and do it well. However, for this to be really useful, there should be a facility such as is provided by unix shells. Unix shells allow the composition of small commands using the following methods

* Composition: `cmd1 | cmd2 | cmd2`
* AndAnd: `cmd1 && cmd2 && cmd3`
* Batch: `cmd1; cmd2; cmd3 > out` or `(cmd1; cmd2; cmd3) > out`

Also, you should be able to use these methods of composition in any combination and also in a nexted manner - `(cmd1 | cmd2) && cmd3`. Gilmour enables you to do just that. See `examples/composition.rb`.

## Protocol

Gilmour uses it's own simple protocol to send request and response "headers".
The structure of the payload is a simple JSON as shown below:

    {
	  data: The actual payload,
	  sender: The origin of the request (unique for each request),
	  code: The response code if this is a response. This uses HTTP error codes
    }

Gilmour has builtin support for error publishing. The error channel is separate from the response channels and everything that is put on the error channel is prefixed by the per-request unique sender uuid, which makes debugging easier. The error packet protocol is

    {
		code: response code (non-200)
		sender: the "sender" from the request
		topic: the topic to which the request was sent
		request_data: the request payload
		userdata: implementation dependent debug infomation
		backtrace: the backtrace of the error
		timestamp: the timestamp of the error
    }

## Redis backend conventions

The `sender` field actually represents a unique sender key. Any reponse that
is to be sent to the request is sent on the "reservered" topic
`response.<sender>` on the same exchange.

For the request-reply pattern, the topic is prefixed with `gilmour.request`.
For the signal-slot pettern, the topic is prefixed with `gilmour.slot`.

The topic on which errors are published is `gilmour.error`.

## Auto-loading

Gilmour code can be structured such that the subscribers are auto-loaded from the filesystem. This enables creation of containers which house multiple microservices, with a choice of which ones to enable. See `examples/container.rb`.

## Health monitoring

Gilmour supports health monitoring heartbeats. Every gilmour process, which has health monitoring enabled, listens for hearbeat pings and responds accordingly. External monitors can use this to monitor the health. [The health bulletin](https://github.com/gilmour-libs/health-bulletin) and [the health-tools](	https://github.com/gilmour-libs/health-tools) are examples of such tools.

## More Usage Examples

See the `examples` directory 
  
## Specs

To run the specs, set `REDIS_HOST` (default is localhost) and `REDIS_PORT` (default is 6379)
to point to your rabbitmq or redis server.
Then, from within the `test` directory, run `rspec spec/*`

