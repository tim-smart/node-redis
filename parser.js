var utils = require('./utils');

var RedisParser = function RedisParser () {
  this.buffer    = null;
  this.reply     = null;
  this.expected  = null;
  this.multi     = null;
  this.replies   = null;
  this.pos       = null;
  this.flag      = 'TYPE';
  this.data      = '';
  this.last_data = null;

  process.EventEmitter.call(this);

  return this;
};

module.exports = RedisParser;

RedisParser.prototype = Object.create(process.EventEmitter.prototype);

(function () {
  // Reset state, no matter where we are at.
  this.resetState = function resetState () {
    this.buffer    = null;
    this.reply     = null;
    this.expected  = null;
    this.multi     = null;
    this.replies   = null;
    this.pos       = null;
    this.flag      = 'TYPE';
    this.data      = '';
    this.last_data = null;
  };

  // Handle an incoming buffer.
  this.onIncoming = function onIncoming (buffer) {
    var char_code,
        pos    = this.pos || 0,
        length = buffer.length;

    // Make sure the buffer is joint properly.
    if ('TYPE' !== this.flag && '' !== this.data) {
      pos--;
      char_code = this.data[this.data.length - 1].charCodeAt(0);
      this.data = this.data.slice(0, -1);
    }

    for (; length > pos;) {
      switch (this.flag) {
      case 'TYPE':
        // What are we doing next?
        switch (buffer[pos++]) {
        // Single line status reply.
        case 43: // + SINGLE
          this.flag = 'SINGLE';
          break;

        // Tells us the length of upcoming data.
        case 36: // $ LENGTH
          this.flag = 'BULK_LENGTH';
          break;

        // Tells us how many args are coming up.
        case 42: // * MULTI
          this.flag = 'MULTI_BULK';
          break;

        case 58: // : INTEGER
          this.flag = 'INTEGER';
          break;

        // Errors
        case 45: // - ERROR
          this.flag = 'ERROR';
          break;
        }
        // Fast forward a char.
        char_code = buffer[pos];
        break;

      // Single line status replies.
      case 'SINGLE':
      case 'ERROR':
        // Add char to the data
        this.data += String.fromCharCode(char_code);
        pos++;

        // Optimize for the common use case.
        if ('O' === this.data && 75 === buffer[pos]) { // OK
          // Send off the reply.
          this.data = 'OK';
          this.onData();

          pos += 3; // Skip the `K\r\n`

          // break early.
          break;
        }

        // Otherwise check for CR
        if (13 === (char_code = buffer[pos])) { // \r CR
          // Send the reply.
          if ('SINGLE' === this.flag) {
            this.onData();
          } else {
            this.onError();
          }

          // Skip \r\n
          pos += 2;
        }
        break;

      // We have a integer coming up. Look for a CR
      // then assume that is the end.
      case 'BULK_LENGTH':
        // We are still looking for more digits.
        // char_code already set by TYPE state.
        this.data += String.fromCharCode(char_code);
        pos++;

        // Is the next char the end? Set next char_code while
        // we are at it.
        if (13 === (char_code = buffer[pos])) { // \r CR
          // Cast to int
          this.data = +this.data;

          // Null reply?
          if (-1 !== this.data) {
            this.flag      = 'BULK';
            this.last_data = this.data;
            this.data      = '';
          } else {
            this.data = null;
            this.onData();
          }

          // Skip the \r\n
          pos += 2;
        }
        break;

      // Short bulk reply.
      case 'BULK':
        if (null === this.buffer && length >= (pos + this.last_data)) {
          // Slow slice is slow.
          if (15 > this.last_data) {
            this.data = new Buffer(this.last_data);
            for (var i = 0; i < this.last_data; i++) {
              this.data[i] = buffer[i + pos];
            }
          } else {
            this.data = buffer.slice(pos, this.last_data + pos);
          }

          // Push back data.
          pos += this.last_data + 2;
        } else if (null !== this.buffer) {
          // Still joining. pos = amount left to go.
          if (pos <= length) {
            if (15 > this.last_data) {
              utils.copyBuffer(buffer, this.buffer, this.last_data - pos, 0, pos);
            } else {
              buffer.copy(this.buffer, this.last_data - pos, 0, pos)
            }

            this.data   = this.buffer;
            this.buffer = null;
            this.pos    = null;

            pos += 2;
          } else {
            if (15 > this.last_data) {
              utils.copyBuffer(buffer, this.buffer, 0, 0);
            } else {
              buffer.copy(this.buffer, 0, 0);
            }
            // More to go.
            pos = (this.last_data - (length - pos)) + length;
            break;
          }
        } else {
          // We will have to do a join.
          this.buffer = new Buffer(this.last_data);
          if (15 > this.last_data) {
            utils.copyBuffer(buffer, this.buffer, 0, pos);
          } else {
            buffer.copy(this.buffer, 0, pos)
          }

          // Point pos to the amount we need.
          pos = (this.last_data - (length - pos)) + length;
          break;
        }

        this.onData();
        break;

      // How many bulk's are coming?
      case 'MULTI_BULK':
        // We are still looking for more digits.
        // char_code already set by TYPE state.
        this.data += String.fromCharCode(char_code);
        pos++;

        // Is the next char the end? Set next char_code while
        // we are at it.
        if (13 === (char_code = buffer[pos])) { // \r CR
          // Cast to int
          this.data = +this.data;

          // Are we multi?
          if (null === this.expected) {
            this.expected = this.data;
            this.reply    = [];
          } else {
            this.multi    = this.expected;
            this.expected = this.data;
            this.replies  = [];
          }

          this.flag      = 'TYPE';
          this.last_data = this.data;
          this.data      = '';

          // Skip the \r\n
          pos += 2;

          // TODO: Optimize for common use case, a bulk reply?
          // Will have to look ahead to check for another MULTI in case
          // we are a multi transaction.
        }
        break;

      case 'INTEGER':
        // We are still looking for more digits.
        // char_code already set by TYPE state.
        this.data += String.fromCharCode(char_code);
        pos++;

        // Is the next char the end? Set next char_code while
        // we are at it.
        if (13 === (char_code = buffer[pos])) { // \r CR
          // Cast to int
          this.data = +this.data;
          this.onData();

          // Skip the \r\n
          pos += 2;
        }
        break;
      }
    }

    // In case we have multiple packets.
    this.pos = pos - length;
  };

  // When we have recieved a chunk of response data.
  this.onData = function onData () {
    if (null !== this.expected) {
      // Decrement the expected data replies and add the data.
      this.reply.push(this.data);
      this.expected--;

      // Finished? Send it off.
      if (0 === this.expected) {
        if (null !== this.multi) {
          this.replies.push(this.reply);
          this.reply = this.expected = null;
          this.multi--;

          if (0 === this.multi) {
            this.emit('reply', this.replies);
            this.replies = this.multi = null;
          }
        } else {
          this.emit('reply', this.reply);
          this.reply = this.expected = null;
        }
      }
    } else {
      if (null === this.multi) {
        this.emit('reply', this.data);
      } else {
        this.replies.push(this.data);
        this.multi--;

        if (0 === this.multi) {
          this.emit('reply', this.replies);
          this.replies = this.multi = null;
        }
      }
    }

    this.last_data = null;
    this.data      = '';
    this.flag      = 'TYPE';
  };

  // Found an error.
  this.onError = function onError () {
    if (null === this.multi) {
      this.emit('error', this.data);
    } else {
      this.replies.push(this.data);
      this.multi--;

      if (0 === this.multi) {
        this.emit('reply', this.replies);
        this.replies = this.multi = null;
      }
    }

    this.last_data = null;
    this.data      = '';
    this.flag      = 'TYPE';
  };
}).call(RedisParser.prototype);
