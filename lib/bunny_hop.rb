require "bunny_hop/version"
require "bunny"

class BunnyHop
  def initialize
    @connection = ::Bunny.new(host: ENV.fetch('RABBITMQ_HOST', 'rabbitmq'), user: ENV.fetch('RABBITMQ_USER', 'rabbitmq'), password: ENV.fetch('RABBITMQ_PASSWORD', 'rabbitmq'))
    @connection.start
    @channel = @connection.create_channel
  end

  def publish(queue, msg)
    lock = Mutex.new
    condition = ConditionVariable.new
    response = nil
    call_id = "#{rand}#{rand}#{rand}"
    exchange = @channel.default_exchange

    reply_queue = @channel.queue('', exclusive: true)
    reply_queue.subscribe do |_delivery_info, properties, payload|
      if properties[:correlation_id] == call_id
        response = payload

        lock.synchronize { condition.signal }
      end
    end

    exchange.publish(msg,
      routing_key: queue,
      correlation_id: call_id,
      reply_to: reply_queue.name)

    Timeout::timeout(5) {
      lock.synchronize { condition.wait(lock) }
    }
  
    @channel.close
    @connection.close

    response
  rescue Timeout::Error => e
    @channel.close
    @connection.close
    raise e
  end
  
  def subscribe(queue)
    queue = @channel.queue(queue)
    exchange = @channel.default_exchange

    queue.subscribe do |_delivery_info, properties, payload|
      puts _delivery_info
      puts properties
      puts payload
      
      result = yield

      exchange.publish(
        result.to_s,
        routing_key: properties.reply_to,
        correlation_id: properties.correlation_id
      )
    end
  end
end
