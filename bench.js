var redis  = require('./'),
    //redis2 = require('redis'),
    Seq    = require('parallel').Sequence,
    assert = require('assert');

var clients = { 'node-redis': redis.createClient(), /*'node_redis': redis2.createClient(),*/ }

var iterations = 10000,
    number     = 10;

var benches = {
  set: function (client, callback) {
    for (var i = 0; i < iterations - 1; i++) {
      client.set('bench' + i, 'xxx');
    }
    client.set('bench' + i, 'xxx', callback);
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
  lpush: function (client, callback) {
    for (var i = 0; i < iterations - 1; i++) {
      client.lpush('bench', 'foo' + i);
    }
    client.lpush('bench', 'foo' + i, callback);
  },
  lrange: function (client, callback) {
    for (var i = 0; i < iterations - 1; i++) {
      client.lrange('bench', 0, 99);
    }
    client.lrange('bench', 0, 99, callback);
  },
};

var task = new Seq();

for (var i = 0; i < number; i++) {
  Object.keys(clients).forEach(function (client) {
    client = clients[client];
    client.benches = {};

    Object.keys(benches).forEach(function (bench) {
      client.benches[bench] = [];

      task.add(function (next, error) {
        process.stdout.write('.');
        var time = Date.now();
        benches[bench](client, function () {
          client.benches[bench].push(Date.now() - time);
          client.flushall(next);
        });
      });
    });
  });;
}

clients['node-redis'].on('connect', function () {
  task.run(end);
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
  keys.forEach(function (client) { clients[client].end(); });
};
