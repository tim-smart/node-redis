var net    = require('net'),
    utils  = require('./utils'),
    Parser = require('./parser');

var RedisClient = function RedisClient(port, host) {
  this.stream         = net.createConnection(port, host);;
  // Command queue.
  this.commands       = new utils.Queue();
  // For the retry timer.
  this.retry          = false;
  this.retry_attempts = 0;
  this.retry_delay    = 250;
  this.retry_backoff  = 1.7;
  // If we want to quit.
  this.quitting       = false;

  var self = this;

  this.stream.on("connect", function () {
    // Reset the retry backoff.
    self.retry       = false;
    self.retry_delay = 250;
    self.stream.setNoDelay();
    self.stream.setTimeout(0);

    // Resend commands.
    var command,
        commands = self.commands.toArray();

    for (var i = 0, il = commands.length; i < il; i++) {
      command = commands[i];
      self.sendCommand(command[0], command[1], command[2]);
    }

    // give connect listeners a chance to run first in case they need to auth
    self.emit("connect");
  });

  this.stream.on("data", function (buffer) {
    try {
      self.parser.onIncoming(buffer);
    } catch (err) {
      self.emit("error", err);
      // Reset state.
      self.parser.resetState();
    }
  });

  this.stream.on("error", function (error) {
    self.emit("error", error);
  });

  this.stream.on("close", function () {
    // Reset the parser state
    this.parser.resetState();

    // Ignore if we are already retrying. Or we want to quit.
    if (self.retry) return;
    self.emit('close');
    if (self.quitting) return;

    self.onDisconnect();
  });

  // Setup the parser.
  this.parser = new Parser();

  this.parser.on('reply', function (reply) {
    self.onReply(reply);
  });

  this.parser.on('error', function (error) {
    console.log('error');
    console.log(error);
    self.emit('error', error);
  });

  // FIXME: TODO: Remove.
  //this.stream.write = function (data) {
    //if (Buffer.isBuffer(data)) {
      //console.log('write:buffer', data.toString());
    //} else {
      //console.log('write', data);
    //}
  //};

  process.EventEmitter.call(this);

  return this;
};

RedisClient.prototype = Object.create(process.EventEmitter.prototype);

RedisClient.prototype.onDisconnect = function (error) {
  var self = this;

  // Make sure the stream is reset.
  this.stream.destroy();

  // Increment the attempts, so we know what to set the timeout to.
  this.retry_attempts++;

  // Move pending commands to the retry queue, and reset.
  this.commands = new utils.Queue();

  // Set the retry timer.
  setTimeout(function () {
    self.stream.connect(self.port, self.host);
  }, this.retry_delay);

  this.retry_delay *= this.retry_backoff;
  this.retry        = true;
};

RedisClient.prototype.onReply = function (reply) {
  var command = this.commands.shift();

  if (command[2]) command[2](reply);
};

RedisClient.prototype.sendCommand = function (command, args, callback) {
  // Commands are uppercase.
  command = command.toUpperCase();

  // Push the command to the stack.
  this.commands.push([command, args, callback]);

  // Can we write?
  if (false === this.stream.writable) return;

  // Do we have to send a multi bulk command?
  // Assume it is a valid command for speed reasons.
  var buffer;
  if (args) {
    var arg, arg_type,
        previous = '',
        length   = args.length + 1; // Don't forget command itself!

    if (!(buffer = multi_buffers[length])) {
      buffer = allocMultiBuffer(length);
    }

    buffer = buffer[command];

    // Write the bulk count.
    utils.writeBuffer(buffer, '' + length, 1);

    // Send the command header.
    this.stream.write(buffer);

    // Send the args. Send as much we can in one go.
    for (var i = 0, il = args.length; i < il; i++) {
      arg      = args[i];
      arg_type = typeof arg;

      if ('string' === arg_type ||
          'number' === arg_type) {
        // We can send this in one go.
        previous += '$' + arg.length + '\r\n' + arg + '\r\n';
      } else if (null === arg || undefined === arg) {
        // Send previous.
        this.stream.write(previous);
        previous = ''
        // Send a nil bulk arg.
        this.stream.write(NIL_BUFFER);
      } else {
        // Assume we are a buffer.
        previous += '$' + buffer.length + '\r\n';
        this.stream.write(previous);
        this.stream.write(buffer);
        previous  = '\r\n';
      }
    }

    // Anything left?
    if ('' !== previous) {
      this.stream.write(previous);
    }
  } else {
    // We are just sending a stand alone command.
    this.stream.write(command_buffers[command]);
  }
};

RedisClient.prototype.quit = RedisClient.prototype.end = function () {
  this.quitting = true;
  return this.sendCommand('QUIT');
};

// Constants
var NIL_BUFFER = new Buffer('$-1\r\n');

// http://code.google.com/p/redis/wiki/CommandReference
exports.commands = [
  // Connection handling
  "QUIT", "AUTH",
  // Commands operating on all value types
  "EXISTS", "DEL", "TYPE", "KEYS", "RANDOMKEY", "RENAME", "RENAMENX", "DBSIZE", "EXPIRE", "TTL", "SELECT",
  "MOVE", "FLUSHDB", "FLUSHALL",
  // Commands operating on string values
  "SET", "GET", "GETSET", "MGET", "SETNX", "SETEX", "MSET", "MSETNX", "INCR", "INCRBY", "DECR", "DECRBY", "APPEND", "SUBSTR",
  // Commands operating on lists
  "RPUSH", "LPUSH", "LLEN", "LRANGE", "LTRIM", "LINDEX", "LSET", "LREM", "LPOP", "RPOP", "BLPOP", "BRPOP", "RPOPLPUSH",
  // Commands operating on sets
  "SADD", "SREM", "SPOP", "SMOVE", "SCARD", "SISMEMBER", "SINTER", "SINTERSTORE", "SUNION", "SUNIONSTORE", "SDIFF", "SDIFFSTORE",
  "SMEMBERS", "SRANDMEMBER",
  // Commands operating on sorted zsets (sorted sets)
  "ZADD", "ZREM", "ZINCRBY", "ZRANK", "ZREVRANK", "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE", "ZCOUNT", "ZCARD", "ZSCORE",
  "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZUNIONSTORE", "ZINTERSTORE",
  // Commands operating on hashes
  "HSET", "HSETNX", "HGET", "HMGET", "HMSET", "HINCRBY", "HEXISTS", "HDEL", "HLEN", "HKEYS", "HVALS", "HGETALL",
  // Sorting
  "SORT",
  // Persistence control commands
  "SAVE", "BGSAVE", "LASTSAVE", "SHUTDOWN", "BGREWRITEAOF",
  // Remote server control commands
  "INFO", "MONITOR", "SLAVEOF", "CONFIG",
  // Publish/Subscribe
  "PUBLISH", "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE",
  // Transactions
  "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH",
  // Undocumented commands
  "PING",
];

// For each command, make a buffer for it.
var command_buffers = {};

exports.commands.forEach(function (command) {
  // Pre-alloc buffers for speed.
  command_buffers[command] = new Buffer('$' + command.length + '\r\n' + command + '\r\n');

  // Don't override stuff.
  if (!RedisClient.prototype[command.toLowerCase()]) {
    RedisClient.prototype[command.toLowerCase()] = function () {
      var args     = utils.toArray(arguments),
          callback = typeof args[args.length - 1] === 'function';

      if (true === callback) {
        callback = args.pop();
      } else {
        callback = null;
      }

      return this.sendCommand(command, args, callback);
    };
  }
});

// Pre-alloc multi bulk command buffers.
var length_buffers = {},
    multi_buffers  = {};

var allocMultiBuffer = function allocMultiBuffer (multi_length) {
  var buffer, length, command, buffers, command_buffer;

  // Check the length and alloc the length in the hash.
  length = ('' + multi_length).length;

  // Exit early if already have alloced the command buffers.
  if (length_buffers[length]) {
    return multi_buffers[multi_length] = length_buffers[length];
  }

  buffers = length_buffers[length] = multi_buffers[multi_length] = {};

  // For every command buffer, make a multi buffer.
  for (var i = 0, il = exports.commands.length; i < il; i++) {
    command        = exports.commands[i];
    command_buffer = command_buffers[command];

    // * + length + \r\n + command.length
    buffer = buffers[command] = new Buffer(1 + length + 2 + command_buffer.length);
    utils.writeBuffer(buffer, '*', 0);
    utils.writeBuffer(buffer, '\r\n', 1 + length);
    utils.copyBuffer(command_buffer, buffer, 1 + length + 2, 0);
  }

  return buffers;
};
