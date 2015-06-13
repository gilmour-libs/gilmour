# Gilmour

Gilmour is a framework for writing micro-services that exchange data over
non-http transports. Currently the supported backend is Redis PubSub.
Redis pubsub channels are used like "routes".
The DSL provided is similar to Sinatra.

## Protocol

Gilmour uses it's own simple protocol to send request and response "headers".
The structure of the payload is a simple JSON as shown below:

    {
	  data: The actual payload,
	  sender: The origin of the request (unique for each request),
	  code: The respoonse code if this is a response
    }

The `sender` field actually represents a unique sender key. Any reponse that
is to be sent to the request is sent on the "reservered" topic
`response.<sender>` on the same exchange.

## Usage Examples

See the `examples` directory for examples of usage

## Specs

To run the specs, set `REDIS_HOST` (default is localhost) and `REDIS_PORT` (default is 6379)
to point to your rabbitmq or redis server.
Then, from within the `test` directory, run `rspec spec/*`

