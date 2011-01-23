var c      = require('../').createClient(),
    assert = require('assert');

var buffer = new Buffer(new Array(1025).join('x'));

module.exports = {
  "test basic commands": function (beforeExit) {
    var afterwards;

    c.set('1', 'test');
    c.get('1', function (error, value) {
      assert.ok(!error);
      assert.equal(value, 'test');
    });

    c.del('1', function (error) {
      assert.ok(!error);
    });

    c.get('1', function (error, value) {
      assert.ok(!error);
      afterwards = value;
    });

    beforeExit(function () {
      assert.isNull(afterwards);
    });
  },
  "test stress": function (beforeExit) {
    var n = 0,
        o = 0;

    for (var i = 0; i < 10000; i++) {
      c.set('2' + i, buffer, function (error) {
        assert.ok(!error);
        ++n;
      });
    }

    for (i = 0; i < 10000; i++) {
      c.del('2' + i, function (error) {
        assert.ok(!error);
        ++o;
      });
    }

    beforeExit(function () {
      assert.equal(n, 10000);
      assert.equal(o, 10000);
    });
  },
  after: function () {
    c.end();
  }
};
