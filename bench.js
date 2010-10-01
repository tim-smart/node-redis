var redis  = require('./'),
    //redis2 = require('redis'),
    Seq    = require('parallel').Sequence,
    assert = require('assert');

var client  = redis.createClient();
//var client2 = redis2.createClient();

var iterations = 10000,
    number     = 10;

var buffer = new Buffer(Array(1025).join('x'));

client.results  = [];
//client2.results = [];

var bench = function bench (client, callback) {
  var time = Date.now();

  for (var i = 0; i < iterations; i++) {
    client.set('bench', 'xxx')
  }

  client.del('bench', function (error) {
    client.results.push(Date.now() - time);
    callback();
  });
};

var task = new Seq();

for (var i = 0; i < number; i++) {
  task.add(function (next, error) {
    process.stdout.write('.');
    bench(client, next);
  });
  //task.add(function (next, error) {
    //process.stdout.write('.');
    //bench(client2, next);
  //});
}

client.on('connect', function () {
  task.run(end);
});

var end = function end () {
  process.stdout.write('\r\n');
  client.results = eval(client.results.join('+'));
  //client2.results = eval(client2.results.join('+'));
  console.log('redis-node avg', client.results);
  console.log('redis-node ops/s', (iterations * number) / client.results * 1000);
  //console.log('redis_node avg', client2.results);
  //console.log('redis_node ops/s', (iterations * number) / client2.results * 1000);
  //if (client.results < client2.results) {
    //console.log('redis-node was ' + (client2.results / client.results) + ' times faster');
  //} else {
    //console.log('redis_node was ' + (client.results / client2.results) + ' times faster');
  //}
  client.end();
  //client2.end();
};
