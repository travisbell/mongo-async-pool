# frozen_string_literal: true

# Shorthand for `require "mongo/async/pool"; Mongo::Async::Pool.install!`, e.g. in a Gemfile:
#
#   gem "mongo-async-pool", require: "mongo/async/pool/install"
require_relative "../pool"

Mongo::Async::Pool.install!
