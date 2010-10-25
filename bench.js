var redis  = require('./'),
    redis2 = require('./bench/node_redis'),
    redis3 = require('./bench/redis-node/redis'),
    redis4 = require('./bench/redis-client'),
    Seq    = require('parallel').Sequence,
    assert = require('assert');

var clients = { 'node-redis': redis.createClient(),  'node_redis':        redis2.createClient(),
                'redis-node': redis3.createClient(), 'redis-node-client': redis4.createClient() }

var iterations = 7500,
    number     = 3;

//var buffer = require('fs').readFileSync('binary');
var buffer = new Buffer(Array(1025 * 2).join('x'));
//var buffer = 'Some some random text for the benchmark.';
//var buffer = 'xxx';

var benches = {
  set: function (client, callback) {
    for (var i = 0; i < iterations - 1; i++) {
      client.set('bench' + i, buffer);
    }
    client.set('bench' + i, buffer, callback);
  },
  get: function (client, callback) {
    for (var i = 0; i < iterations - 1; i++) {
      client.get('bench' + i);
    }
    client.get('bench' + i, callback);
  },
  del: function (client, callback) {
    for (var i = 0; i < iterations - 1; i++) {
      client.del('bench' + i);
    }
    client.del('bench' + i, callback);
  },
  //lpush: function (client, callback) {
    //for (var i = 0; i < iterations - 1; i++) {
      //client.lpush('bench', buffer);
    //}
    //client.lpush('bench', buffer, callback);
  //},
  //lrange: function (client, callback) {
    //for (var i = 0; i < iterations - 1; i++) {
      //client.lrange('bench', 0, 99);
    //}
    //client.lrange('bench', 0, 99, callback);
  //},
  //hmset: function (client, callback) {
    //if ('redis-node' === client._name) return callback();
    //for (var i = 0; i < iterations - 1; i++) {
      //client.hmset('bench' + i, 'key', buffer, 'key2', buffer);
    //}
    //client.hmset('bench' + i, 'key', buffer, 'key2', buffer, callback);
  //},
  //hmget: function (client, callback) {
    //if ('redis-node' === client._name) return callback();
    //for (var i = 0; i < iterations - 1; i++) {
      //client.hmget('bench' + i, 'key', 'key2');
    //}
    //client.hmget('bench' + i, 'key', 'key2', callback);
  //},
};

var task   = new Seq(),
    warmup = new Seq();

Object.keys(clients).forEach(function (client) {
  clients[client]._name = client;
  client                = clients[client];
  client.benches        = {};

  for (var i = 0; i < number; i++) {
    Object.keys(benches).forEach(function (bench) {
      client.benches[bench] = [];

      task.add(function (next, error) {
        process.stdout.write('.');
        var time = Date.now();
        benches[bench](client, function (error) {
          client.benches[bench].push(Date.now() - time);
          next();
        });
      });
    });

    task.add(function (next) {
      client.flushall(next);
    });
  }
});
Object.keys(clients).forEach(function (client) {
  clients[client]._name = client;
  client                = clients[client];
  client.benches        = {};

  Object.keys(benches).forEach(function (bench) {
    client.benches[bench] = [];

    warmup.add(function (next) {
      client.flushall(next);
    });
  });

  warmup.add(function (next) {
    clients['node-redis'].del('bench', next);
  });
});

clients['node-redis'].on('connect', function () {
  var old_iter = iterations;

  iterations = 100;

  warmup.run(function () {
    //throw new Error;
    iterations = old_iter;
    setTimeout(function () {
      task.run(end);
    }, 1000);
  });
});

var end = function end () {
  process.stdout.write('\r\n');
  var bench, client_name, client,
      keys       = Object.keys(clients),
      bench_keys = Object.keys(benches);

  for (var i = 0, il = bench_keys.length; i < il; i++) {
    bench = bench_keys[i];
    console.log('=== ' + bench + ' x' + iterations + ' ===');

    for (var j = 0, jl = keys.length; j < jl; j++) {
      client_name = keys[j];
      client      = clients[client_name];

      console.log(client_name + ' results: ' + client.benches[bench].join(', '));
    }

    console.log('');

    for (j = 0, jl = keys.length; j < jl; j++) {
      client_name = keys[j];
      client      = clients[client_name];

      client.benches[bench] = eval(client.benches[bench].join('+')) / client.benches[bench].length;

      console.log(client_name + ' avg: ' + client.benches[bench]);
    }

    console.log('');

    for (j = 0, jl = keys.length; j < jl; j++) {
      client_name = keys[j];
      client      = clients[client_name];

      console.log(client_name + ' ops/s: ' + ((iterations / client.benches[bench]) * 1000));
    }

    console.log('\r\n');
  }

  // Bye!
  process.exit();
};
