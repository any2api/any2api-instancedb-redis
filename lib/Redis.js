var RedisPool = require('redis-poolify');
var _ = require('lodash');



module.exports = function(spec) {
  var obj = {};

  spec = spec || {};

  var clientPool = RedisPool();

  var wrap = function(value, type) {
    if (type === 'json_object' || type === 'json_array' || type === 'boolean' || type === 'number') {
      value = JSON.stringify(value);
    } else if (Buffer.isBuffer(value)) { // && type === 'binary'
      value = value.toString('base64'); //value.toJSON()
    }

    return value;
  };

  var unwrap = function(value, type) {
    if (!value) return null;

    if (type === 'json_object' || type === 'json_array' || type === 'boolean' || type === 'number') {
      value = JSON.parse(value);
    } else if (type === 'binary') {
      value = new Buffer(value, 'base64');
    }

    return value;
  };

  var set = function(key, value, type, callback) {
    value = wrap(value, type);

    clientPool.acquire(spec, function(err, client) {
      if (err) return callback(err);

      client.set(key, value, function(err) {
        callback(err);

        clientPool.release(spec, client);
      });
    });
  };

  var remove = function(key, callback) {
    clientPool.acquire(spec, function(err, client) {
      if (err) return callback(err);

      client.del(key, function(err) {
        callback(err);
        
        clientPool.release(spec, client);
      });
    });
  };

  var removeAll = function(prefix, callback) {
    clientPool.acquire(spec, function(err, client) {
      if (err) return callback(err);

      client.keys(prefix + '*', function(err, keys) {
        if (err || _.isEmpty(keys)) {
          callback(err);

          clientPool.release(spec, client);

          return;
        }

        client.del(keys, function(err) {
          callback(err);

          clientPool.release(spec, client);
        });
      });
    });
  };

  var list = function(prefix, callback) {
    clientPool.acquire(spec, function(err, client) {
      if (err) return callback(err);

      client.keys(prefix + '*', function(err, keys) {
        if (err) {
          callback(err);

          clientPool.release(spec, client);

          return;
        }

        var list = [];

        _.each(keys, function(key) {
          list.push(key.split(':').pop());
        });

        callback(null, list);

        clientPool.release(spec, client);
      });
    });
  };

  var get = function(key, type, callback) {
    clientPool.acquire(spec, function(err, client) {
      if (err) return callback(err);

      client.get(key, function(err, value) {
        value = unwrap(value, type);

        callback(err, value);

        clientPool.release(spec, client);
      });
    });
  };

  var getAll = function(prefix, type, callback) {
    clientPool.acquire(spec, function(err, client) {
      if (err) return callback(err);

      client.keys(prefix + '*', function(err, keys) {
        if (err) {
          callback(err);

          clientPool.release(spec, client);

          return;
        }

        var transaction = client.multi();

        _.each(keys, function(key) {
          transaction.get(key);
        });

        //TODO: filter by status here already (efficiency)

        transaction.exec(function(err, values) {
          if (err) {
            callback(err);

            clientPool.release(spec, client);

            return;
          }

          var mappedValues = {};

          _.each(keys, function(key) {
            var value = values.shift();
            var name = key.split(':').pop();

            var t = type;
            if (_.isPlainObject(type)) t = type[name];

            mappedValues[name] = unwrap(value, t);
          });
          
          callback(null, mappedValues);
          
          clientPool.release(spec, client);
        });
      });
    });
  };



  obj.set = set;
  obj.remove = remove;
  obj.removeAll = removeAll;
  obj.list = list;
  obj.get = get;
  obj.getAll = getAll;

  return obj;
};
