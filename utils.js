
// noop to keep references low.
exports.noop = function () {};
  
// Fast copyBuffer method for small buffers.
exports.copyBuffer = function copyBuffer (source, target, start, s_start, s_end) {
  s_end || (s_end = source.length);

  for (var i = s_start; i < s_end; i++) {
    target[i - s_start + start] = source[i];
  }

  return target;
};

// Fast write buffer for small uns.
var writeBuffer = exports.writeBuffer = function writeBuffer (buffer, string, offset) {
  for (var i = 0, il = string.length; i < il; i++) {
    buffer[i + offset] = string.charCodeAt(i);
  }

  return il;
};

var toArray = exports.toArray = function toArray (args) {
  var len = args.length,
      arr = new Array(len), i;

  for (i = 0; i < len; i++) {
    arr[i] = args[i];
  }

  return arr;
};

// Queue class adapted from Tim Caswell's pattern library
// http://github.com/creationix/pattern/blob/master/lib/pattern/queue.js
var Queue = function () {
  this.tail   = [];
  this.head   = toArray(arguments);
  this.offset = 0;
};

exports.Queue = Queue;

Queue.prototype.shift = function () {
  if (this.offset === this.head.length) {
    var tmp = this.head;
    tmp.length = 0;
    this.head = this.tail;
    this.tail = tmp;
    this.offset = 0;
    if (this.head.length === 0) {
      return;
    }
  }
  return this.head[this.offset++];
};

Queue.prototype.push = function (item) {
  return this.tail.push(item);
};

Queue.prototype.toArray = function () {
    var array = this.head.slice(this.offset);
    array.push.apply(array, this.tail);
    return array;
};
