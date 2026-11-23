# frozen_string_literal: true

require "io/wait"

module Mongo
  module Async
    module Pool
      # Prepended to Mongo::Socket by {Mongo::Async::Pool.install!}.
      #
      # Mongo::Socket waits for its socket with Kernel.select, which under a
      # fiber scheduler goes through Scheduler#io_select. Async implements that by
      # spawning a thread per call, and the driver hits it on nearly every read
      # (the reply is rarely there yet when read_nonblock is first tried).
      #
      # When a fiber scheduler is active, this waits with IO#wait_readable /
      # IO#wait_writable instead, which the scheduler handles in its event loop.
      # Otherwise (e.g. the driver's monitor threads) it falls back to the stock
      # code.
      #
      # It also makes fewer system calls per round trip than the stock code:
      #
      # - After a request is written, the first read waits for the reply rather
      #   than trying a read that would almost always find nothing yet.
      # - Small reads (such as a reply's 16-byte header) read ahead as much as is
      #   available, so the rest of a small reply is already in memory when the
      #   driver asks for it.
      # - Writes use write_nonblock, rather than IO#write, and reads from plain
      #   sockets use recv_nonblock, rather than IO#read_nonblock: both of the
      #   latter go through the scheduler's hooks, which cost an extra system call
      #   and an allocation.
      #
      # The stock read also selects on the pool generation's pipe, so that
      # clearing a pool can wake blocked readers. That isn't needed here: on Ruby
      # 3.5+, closing the socket raises IOError in a fiber blocked in
      # wait_readable, which the driver maps to a SocketError.
      #
      # Connection establishment (TCP/TLS connect) still uses IO.select. It only
      # happens when a pool opens a new connection, so it isn't worth overriding.
      module SocketExtension
        # The size of a socket's read-ahead buffer (allocated on its first read),
        # and the most a small read asks for. Also the largest TLS record.
        READ_AHEAD_SIZE = 16_384

        # Same as Mongo::Socket::WRITE_CHUNK_SIZE.
        WRITE_CHUNK_SIZE = 65_536

        def write(*args, timeout: nil)
          result = super
          @awaiting_reply = true
          result
        end

        private

        def read_from_socket(length, socket_timeout: nil, csot: false)
          return allocate_string(0) if length == 0
          # Bytes read ahead come first, scheduler or not. Returning fewer bytes than asked for is fine: the driver reads until it has them all.
          return take_read_ahead(length) if read_ahead?
          return super unless Fiber.scheduler

          _timeout = socket_timeout || timeout
          deadline = nil
          if _timeout
            if _timeout > 0
              deadline = ::Mongo::Utils.monotonic_time + _timeout
            elsif _timeout < 0
              raise_timeout_error!("Negative timeout #{_timeout} given to socket", csot)
            end
          end

          if @awaiting_reply
            @awaiting_reply = false
            # OpenSSL may already hold decrypted bytes that the TCP socket won't signal.
            wait_for_socket(:wait_readable, deadline, _timeout, csot) unless @socket.respond_to?(:pending) && @socket.pending > 0
          end

          return read_ahead(length, deadline, _timeout, csot) if length < READ_AHEAD_SIZE

          buf_size = read_buffer_size
          buf_size = length if length < buf_size

          buf = allocate_string(buf_size)
          data = nil
          retrieved = 0

          while retrieved < length
            retrieve = length - retrieved
            retrieve = buf_size if retrieve > buf_size

            # Reading from a TLS socket may need to write first, so it can ask to wait for either.
            case (chunk = socket_read_nonblock(retrieve, buf))
            when :wait_readable, :wait_writable
              wait_for_socket(chunk, deadline, _timeout, csot)
              next
            when nil
              raise EOFError, "end of file reached"
            end

            # Read everything in one go: return the buffer as is, saving an allocation and a copy.
            return chunk if retrieved == 0 && chunk.length == length

            data = allocate_string(length) if data.nil?
            data[retrieved, chunk.length] = chunk
            retrieved += chunk.length
          end

          data
        end

        # Same as the stock write without a timeout, except for write_nonblock.
        def write_without_timeout(*args)
          return super unless Fiber.scheduler

          args.each do |buf|
            buf = buf.to_s
            size = buf.bytesize
            offset = 0

            while offset < size
              chunk = offset == 0 && size <= WRITE_CHUNK_SIZE ? buf : buf.byteslice(offset, WRITE_CHUNK_SIZE)

              case (written = @socket.write_nonblock(chunk, exception: false))
              when :wait_writable, :wait_readable
                @socket.to_io.public_send(written)
              else
                offset += written
              end
            end
          end
        end

        def wait_for_socket_to_be_writable(deadline)
          return super unless Fiber.scheduler

          wait_timeout = deadline - ::Mongo::Utils.monotonic_time
          wait_timeout > 0 && !@socket.to_io.wait_writable(wait_timeout).nil?
        end

        def wait_for_socket(event, deadline, timeout, csot)
          wait_timeout = nil

          if deadline
            wait_timeout = deadline - ::Mongo::Utils.monotonic_time
            raise_timeout_error!("Took more than #{timeout} seconds to receive data", csot) if wait_timeout <= 0
          end

          # An SSLSocket isn't an IO, so wait on the underlying TCP socket.
          return if @socket.to_io.public_send(event, wait_timeout)

          raise_timeout_error!("Took more than #{timeout} seconds to receive data (wait timed out)", csot)
        end

        # IO#read_nonblock goes through the fiber scheduler (its io_read hook), which
        # costs an IO::Buffer and an extra system call; recv_nonblock is a plain
        # recv(2). Nothing reads through the IO's own buffer, which recv would
        # bypass. An SSLSocket reads its socket directly.
        def socket_read_nonblock(size, buffer)
          if @socket.is_a?(::BasicSocket)
            @socket.recv_nonblock(size, 0, buffer, exception: false)
          else
            @socket.read_nonblock(size, buffer, exception: false)
          end
        end

        def read_ahead?
          @read_ahead_offset && @read_ahead_offset < @read_ahead.bytesize
        end

        # Read whatever is available, up to READ_AHEAD_SIZE, and return up to length bytes of it.
        def read_ahead(length, deadline, timeout, csot)
          buffer = (@read_ahead ||= allocate_string(READ_AHEAD_SIZE))

          loop do
            case (chunk = socket_read_nonblock(READ_AHEAD_SIZE, buffer))
            when :wait_readable, :wait_writable
              wait_for_socket(chunk, deadline, timeout, csot)
            when nil
              raise EOFError, "end of file reached"
            else
              break
            end
          end

          @read_ahead_offset = 0
          take_read_ahead(length)
        end

        # Copies out of the buffer: a slice running to its end would share it, and
        # the next read into it would then have to copy it.
        def take_read_ahead(length)
          buffer = @read_ahead
          offset = @read_ahead_offset
          available = buffer.bytesize - offset

          if length >= available
            @read_ahead_offset = nil
            buffer.unpack1("a*", offset: offset)
          else
            @read_ahead_offset = offset + length
            buffer.byteslice(offset, length)
          end
        end
      end
    end
  end
end
