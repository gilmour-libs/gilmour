# Gilmour

Gilmour is a framework for writing micro-services that exchange data over
non-http transports. Currently supported backends are AMQP and Redis PubSub.
In both cases, topics (in case of AMQP) and channels (in case of Redis) are
used like "routes".
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

To run the specs, modify `test/spec/helpers/amqp.yml` to point to your rabbitmq or redis server.
Then, from within the `test` directory, run `rspec spec/*`

