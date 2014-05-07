# Gilmour

Gilmour is a framework that helps in creating services that communicate over
Redis pub/sub and/or AMQP topic exchanges. The idea is to use topics as "routes" and
provide a DSL similar to Sinatra to write services that communicate via the
message bus.

## Protocol

Gilmour uses it's own simple protocol to send request and response "headers".
The structure of the AMQP payload is a simple JSON as shown below:

    {
	  data: The actual payload,
	  sender: The origin of the request (unique for each request),
	  code: The respoonse code if this is a response
    }

The `sender` field actually represents a unique sender key. Any reponse that
is to be sent to the request is sent on the "reservered" topic
`response.<sender>` on the same exchange.

## Usage

container.rb

	require 'gilmour'
	
	class MyContainer
	  include Gilmour::Base
	
	  subscribers_path "./subscribers"
	
	  def initialize(options)
        spawn = true
	    start(options, spawn)
        my_other_cool_functionality
	  end
	end

subscribers/testsubscriber.rb

	class MySubscriber < MyContainer
	  listen_to "test.topic.*" do
	    respond 'Pong!' if request.body == 'Ping!'
	  end
	end

## Specs

To run the specs, modify `test/spec/helpers/amqp.yml` to point to your rabbitmq or redis server.
Then, from within the `test` directory, run `rspec spec/*`

