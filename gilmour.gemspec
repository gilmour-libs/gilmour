# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "./version"

Gem::Specification.new do |s|
  s.name        = "gilmour"
  s.version     = Gilmour::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Aditya Godbole", "Piyush Verma"]
  s.email       = ["code.aa@gdbl.me", "piyush@piyushverma.net"]
  s.homepage    = ""
  s.summary     = %q{A Sinatra like DSL for implementing AMQP services}
  s.description = %q{This gem provides a Sinatra like DSL and a simple protocol to enable writing services that communicate over AMQP}

  s.add_development_dependency "rspec"
  s.add_development_dependency "rspec-given"
  s.add_dependency "mash"
  s.add_dependency "redis"
  s.add_dependency "gilmour-em-hiredis"
  s.add_dependency "amqp"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
end
