# frozen_string_literal: true

module Mongo
  module Async
    module Pool
      # Prepended to Mongo::Server::ConnectionPool's singleton class by
      # {Mongo::Async::Pool.install!}. The stock pool's finalizer disconnects its idle
      # connections and stops at the first exception; this one disconnects each
      # connection independently and swallows errors (including Interrupt, and
      # Errno::EBADF for sockets already closed, e.g. after a fork), since an
      # exception escaping a finalizer is reported on whatever code GC
      # interrupted.
      module SafeFinalizer
        def finalize(available_connections, pending_connections, populator)
          proc do
            [available_connections, pending_connections].each do |connections|
              connections.each do |connection|
                connection.disconnect!(reason: :pool_closed)
              rescue StandardError, Interrupt
                # Best effort.
              end
            end

            begin
              populator&.stop!
            rescue StandardError, Interrupt
              # Best effort.
            end
          end
        end
      end
    end
  end
end
