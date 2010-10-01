var redis  = require('./'),
    redis2 = require('redis'),
    Seq    = require('parallel').Sequence;

var client  = redis.createClient();
var client2 = redis2.createClient();

var iterations = 50000,
    number     = 5;

client.count  = 0;
client2.count = 0;

var bench = function bench (client, name, callback) {
  var time = Date.now();

  //client.multi();
  for (var i = 0; i < iterations; i++) {
    client.hmset('bench', 'key', 'test', 'heh', 'teehee');
    client.hgetall('bench');
    client.del('bench');
  }
  //client.exec();

  client.del('bench', function (error) {
    console.log(name, Date.now() - time);
    callback();
  });
};

var task = new Seq();

for (var i = 0; i < number; i++) {
  task.add(function (next, error) {
    bench(client, 'one-' + ++client.count, next);
  });
  task.add(function (next, error) {
    bench(client2, 'two-' + ++client2.count, next);
  });
}

client.on('connect', function () {
  task.run(end);
});

var end = function end () {
  client.end();
  client2.end();
};
