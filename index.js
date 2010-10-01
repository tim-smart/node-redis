var net    = require('net'),
    utils  = require('./utils'),
    Parser = require('./parser');

var RedisClient = function RedisClient(port, host) {
  this.stream         = net.createConnection(port, host);;
  this.connected      = false;
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
    self.connected   = true;

    // Resend commands if we need to.
    var command,
        commands = self.commands.toArray();

    self.commands  = new utils.Queue();

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

  // FIXME
  //this.stream.oldWrite = this.stream.write;
  //this.stream.write = function (data) {
    //console.log('>', data);
    //this.oldWrite(data);
  //};

  // Setup the parser.
  this.parser = new Parser();

  var prev;
  this.parser.on('reply', function (reply) {
    var command = self.commands.shift();
    if (!command) {
      console.log(prev);
      console.log(reply);
    }
    prev = command;
    if (command[2]) command[2](null, reply);
  });

  // DB error
  this.parser.on('error', function (error) {
    var command = self.commands.shift();
    error = new Error(error);
    if (command[2]) command[2](error);
    else self.emit('error', error);
  });

  process.EventEmitter.call(this);

  return this;
};

RedisClient.prototype = Object.create(process.EventEmitter.prototype);

// Exports
exports.RedisClient = RedisClient;

// createClient
exports.createClient = function createClient (port, host) {
  return new RedisClient(port || 6379, host);
};

RedisClient.prototype.onDisconnect = function (error) {
  var self = this;

  // Make sure the stream is reset.
  this.connected = false;
  this.stream.destroy();
  this.parser.resetState();

  // Increment the attempts, so we know what to set the timeout to.
  this.retry_attempts++;

  // Set the retry timer.
  setTimeout(function () {
    self.stream.connect(self.port, self.host);
  }, this.retry_delay);

  this.retry_delay *= this.retry_backoff;
  this.retry        = true;
};

// We make some assumptions:
//
// * command WILL be uppercase and valid.
// * args IS an array
RedisClient.prototype.sendCommand = function (command, args, callback) {
  // Push the command to the stack.
  this.commands.push([command, args, callback]);

  // Writable?
  if (false === this.connected) return;

  // Do we have to send a multi bulk command?
  // Assume it is a valid command for speed reasons.
  var args_length;

  if (args && 0 < (args_length = args.length)) {
    var arg, arg_type,
        previous   = '*' + (args_length + 1) + '\r\n' + '$' + command.length + '\r\n' + command + '\r\n',
        has_buffer = false;

    // TODO: Somehow get rid of this - or an alternative.
    for (var i = 0, il = args_length; i < il; i++) {
      if (args[i] instanceof Buffer) {
        has_buffer = true;
        break;
      }
    }

    // Send the args. Send as much we can in one go.
    if (false === has_buffer) {
      for (i = 0, il = args_length; i < il; i++) {
        arg = '' + args[i];
        previous += '$' + arg.length + '\r\n' + arg + '\r\n';
      }

      this.stream.write(previous);
    } else {
      for (i = 0, il = args_length; i < il; i++) {
        arg      = args[i];
        arg_type = typeof arg;

        if ('string' === arg_type) {
          // We can send this in one go.
          previous += '$' + arg.length + '\r\n' + arg + '\r\n';
        } else if ('number' === arg_type) {
          // We can send this in one go.
          previous += '$' + ('' + arg).length + '\r\n' + arg + '\r\n';
        } else if (null === arg || 'undefined' === arg_type) {
          // Send NIL
          this.stream.write(previous + '$-1\r\n');
          previous = ''
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
  // Pre-alloc buffers for non-multi commands.
  command_buffers[command] = new Buffer('*1\r\n$' + command.length + '\r\n' + command + '\r\n');

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
