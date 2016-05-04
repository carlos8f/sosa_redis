var crypto = require('crypto');

module.exports = function (coll_name, backend_options) {
  backend_options || (backend_options = {});

  if (!backend_options.client) throw new Error('must pass a node_redis client with backend_options.client');
  var client = backend_options.client;

  function escapeBase64 (str) {
    return str.replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '')
  }

  function hash (id) {
    return escapeBase64(crypto.createHash('sha1').update(id).digest('base64'))
  }

  var coll_path = backend_options.prefix ? backend_options.prefix + ':' : '';
  coll_path += coll_name;
  if (backend_options.key_prefix && backend_options.key_prefix.length) {
    coll_path += '.' + backend_options.key_prefix.map(hash).join('.')
  }

  function toKey (key_path, hashed_id) {
    var key = coll_path + '.' + key_path;
    if (typeof hashed_id !== 'undefined') key += '.' + hashed_id;
    return key;
  }

  var idx_key = toKey('keys');
  var score_key = toKey('score');

  return {
    load: function (id, opts, cb) {
      try {
        var hashed_id = hash(id);
      }
      catch (e) {
        return cb(e);
      }
      var value_key = toKey('value', hashed_id);
      client.GET(value_key, function (err, raw) {
        if (err) return cb(err);
        if (raw) {
          try {
            var obj = JSON.parse(raw);
          }
          catch (e) {
            return cb(e);
          }
          cb(null, obj);
        }
        else cb(null, null);
      });
    },
    save: function (id, obj, opts, cb) {
      try {
        var raw = JSON.stringify(obj);
        var hashed_id = hash(id);
      }
      catch (e) {
        return cb(e);
      }
      var value_key = toKey('value', hashed_id);

      if (opts.ttl) {
        client.SETEX(value_key, opts.ttl, raw, withSet);
      }
      else client.SET(value_key, raw, withSet);

      function withSet (err) {
        if (err) return cb(err);
        var ret = JSON.parse(raw);

        if (opts.index === false) return cb(null, ret);
        if (typeof opts.score == 'number') {
          withScore(opts.score);
        }
        else {
          client.ZSCORE(idx_key, id, function (err, score) {
            if (err) return cb(err);
            if (score) return cb(null, ret);
            else {
              client.INCR(score_key, function (err, score) {
                if (err) return cb(err);
                withScore(score);
              });
            }
          });
        }

        function withScore (score) {
          client.ZADD(idx_key, score, id, function (err) {
            if (err) return cb(err);
            cb(null, ret);
          });
        }
      }
    },
    destroy: function (id, opts, cb) {
      try {
        var hashed_id = hash(id);
      }
      catch (e) {
        return cb(e);
      }
      var value_key = toKey('value', hashed_id);

      this.load(id, {}, function (err, obj) {
        if (err) return cb(err);
        if (obj === null) {
          // due to ttl, obj might still be a member of idx. cleanup.
          client.ZREM(idx_key, id, function (err) {
            if (err) return cb(err);
            cb(null, null);
          });
          return;
        }
        client.MULTI()
          .DEL(value_key)
          .ZREM(idx_key, id)
          .EXEC(function (err) {
            if (err) return cb(err);
            cb(null, obj);
          });
      });
    },
    select: function (opts, cb) {
      var self = this
        , start = opts.offset || 0
        , stop = opts.limit ? start + opts.limit - 1 : -1

      client[opts.reverse ? 'ZREVRANGE' : 'ZRANGE'](idx_key, start, stop, function (err, chunk) {
        if (err) return cb(err);
        var latch = chunk.length, errored = false;
        if (!latch) return cb(null, chunk);
        chunk.forEach(function (id, idx) {
          self.load(id, opts, function (err, obj) {
            if (errored) return;
            if (err) {
              errored = true;
              return cb(err);
            }
            if (obj === null) {
              // due to ttl, obj might still be a member of idx. cleanup.
              setImmediate(function () {
                client.ZREM(idx_key, id);
              });
            }
            chunk[idx] = obj;
            if (!--latch) {
              return cb(null, chunk.filter(function (obj) {
                return obj !== null;
              }));
            }
          });
        });
      });
    }
  };
};
