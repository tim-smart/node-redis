var utils = require('./utils');

var RedisParser = function RedisParser () {
  this.resetState();

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
    this.data      = null;
    this.last_data = null;
  };

  // Handle an incoming buffer.
  this.onIncoming = function onIncoming (buffer) {
    var char_code,
        pos    = this.pos || 0,
        length = buffer.length;

    // Make sure the buffer is joint properly.
    if ('TYPE' !== this.flag && null !== this.data) {
      // We need to wind back a step.
      // If we have CR now, it would break the parser.
      if (0 < this.data.length) {
        char_code = this.data.charCodeAt(this.data.length - 1);
        this.data = this.data.slice(0, -1);
        --pos;
      } else {
        char_code = buffer[pos];
      }
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
        this.data = '';
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
          if (0 <= this.data) {
            this.flag      = 'BULK';
            this.last_data = this.data;
            this.data      = null;
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

          this.onData();

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

            this.onData();
            pos += 2;
          } else {
            if (15 > this.last_data) {
              utils.copyBuffer(buffer, this.buffer, 0, 0);
            } else {
              buffer.copy(this.buffer, 0, 0);
            }

            // More to go.
            pos = (this.last_data - (length - pos)) + length;
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
        }
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
          this.last_data = +this.data;
          this.data      = null;

          // Are we multi?
          if (null === this.expected) {
            this.expected = this.last_data;
            this.reply    = [];
          }

          // Skip the \r\n
          pos += 2;
          this.flag = 'TYPE';

          char_code = buffer[pos];

          // Will have to look ahead to check for another MULTI in case
          // we are a multi transaction.
          if (36 === char_code) { // $ - BULK_LENGTH
            // We are bulk data.
            this.flag = 'BULK_LENGTH';

            // We are skipping the TYPE check. Skip the $
            pos++;
            // We need to set char code and data.
            char_code = buffer[pos];
            this.data = '';
          } else if (char_code) {
            // Multi trans time.
            this.multi    = this.expected;
            this.expected = null;
            this.replies  = [];
          }
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
          this.multi--;

          if (0 === this.multi) {
            this.emit('reply', this.replies);
            this.replies = this.multi = null;
          }
        } else {
          this.emit('reply', this.reply);
        }
        this.reply = this.expected = null;
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
    this.data      = null;
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
    this.data      = null;
    this.flag      = 'TYPE';
  };
}).call(RedisParser.prototype);
