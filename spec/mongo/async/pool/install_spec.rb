# frozen_string_literal: true

require "open3"
require "rbconfig"

RSpec.describe(Mongo::Async::Pool) do
  def run_ruby(code)
    lib = File.expand_path("../../../../lib", __dir__)
    output, status = Open3.capture2e(RbConfig.ruby, "-I", lib, "-e", code)
    expect(status).to be_success, output
    output.lines.last.strip
  end

  it "changes nothing until installed" do
    result = run_ruby(<<~RUBY)
      require "mongo/async/pool"
      print [Mongo::Async::Pool.installed?, Mongo::Server.ancestors.include?(Mongo::Async::Pool::ServerExtension), Mongo::Socket.ancestors.include?(Mongo::Async::Pool::SocketExtension)].inspect
    RUBY

    expect(result).to eq("[false, false, false]")
  end

  it "installs selectively" do
    result = run_ruby(<<~RUBY)
      require "mongo/async/pool"
      Mongo::Async::Pool.install!(socket: false)
      print [Mongo::Async::Pool.pool_installed?, Mongo::Async::Pool.socket_installed?].inspect
    RUBY

    expect(result).to eq("[true, false]")
  end

  it "installs from mongo/async/pool/install" do
    result = run_ruby(<<~RUBY)
      require "mongo/async/pool/install"
      print Mongo::Server.ancestors.include?(Mongo::Async::Pool::ServerExtension).inspect
    RUBY

    expect(result).to eq("true")
  end

  it "is idempotent" do
    described_class.install!
    expect(Mongo::Server.ancestors.count(Mongo::Async::Pool::ServerExtension)).to eq(1)
  end

  it "installs the socket extension automatically only where closing a socket interrupts fibers" do
    expect(described_class.socket_installed?).to eq(Mongo::Async::Pool::Compatibility.interruptible_io?)
  end

  describe Mongo::Async::Pool::Compatibility do
    it "accepts the installed driver" do
      expect { described_class.check_pool! }.not_to raise_error
      expect { described_class.check_socket! }.not_to raise_error
    end

    it "explains what's missing from an incompatible driver" do
      allow(Mongo::Socket).to receive(:private_method_defined?).and_call_original
      allow(Mongo::Socket).to receive(:private_method_defined?).with(:read_from_socket).and_return(false)

      expect { described_class.check_socket! }.to raise_error(Mongo::Async::Pool::IncompatibleDriverError, /Mongo::Socket#read_from_socket/)
    end
  end

  describe Mongo::Async::Pool::SafeFinalizer do
    it "disconnects every connection even when some raise" do
      erroring = instance_double(Mongo::Server::Connection)
      allow(erroring).to receive(:disconnect!).and_raise(Interrupt)
      healthy = instance_double(Mongo::Server::Connection, disconnect!: true)
      populator = instance_double(Mongo::Server::ConnectionPool::Populator, stop!: true)

      finalizer = Mongo::Server::ConnectionPool.finalize([erroring, healthy], [], populator)

      expect { finalizer.call }.not_to raise_error
      expect(healthy).to have_received(:disconnect!).with(reason: :pool_closed)
      expect(populator).to have_received(:stop!)
    end
  end
end
