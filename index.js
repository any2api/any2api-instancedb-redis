var debug = require('debug')(require('./package.json').name);
var _ = require('lodash');
var async = require('async');
var NeDB = null;
var RedisPool = null;



module.exports = function(spec) {
  var obj = {};

  spec = spec || {};
  spec.scope = spec.scope || 'default';
  spec.forceRedis = spec.forceRedis || false;

  var set, remove, removeAll, list, get, getAll;

  if (spec.redisConfig || spec.forceRedis) {
    spec.redisConfig = spec.redisConfig || {};

    RedisPool = require('redis-poolify');
    var clientPool = RedisPool();

    set = function(key, value, callback) {
      if (_.isPlainObject(value)) value = JSON.stringify(value);

      clientPool.acquire(spec.redisConfig, function(err, client) {
        if (err) return callback(err);

        client.set(key, value, function(err) {
          callback(err);

          clientPool.release(spec.redisConfig, client);
        });
      });
    };

    remove = function(key, callback) {
      clientPool.acquire(spec.redisConfig, function(err, client) {
        if (err) return callback(err);

        client.del(key, function(err) {
          callback(err);
          
          clientPool.release(spec.redisConfig, client);
        });
      });
    };

    removeAll = function(prefix, callback) {
      clientPool.acquire(spec.redisConfig, function(err, client) {
        if (err) return callback(err);

        client.keys(prefix + '*', function(err, keys) {
          if (err || _.isEmpty(keys)) {
            callback(err);

            clientPool.release(spec.redisConfig, client);

            return;
          }

          client.del(keys, function(err) {
            callback(err);

            clientPool.release(spec.redisConfig, client);
          });
        });
      });
    };

    list = function(prefix, callback) {
      clientPool.acquire(spec.redisConfig, function(err, client) {
        if (err) return callback(err);

        client.keys(prefix, function(err, keys) {
          if (err) {
            callback(err);

            clientPool.release(spec.redisConfig, client);

            return;
          }

          var list = [];

          _.each(keys, function(key) {
            list.push(key.split(':').pop());
          });

          callback(null, list);

          clientPool.release(spec.redisConfig, client);
        });
      });
    };

    get = function(key, callback) {
      clientPool.acquire(spec.redisConfig, function(err, client) {
        if (err) return callback(err);

        client.get(key, function(err, value) {
          try {
            var parsed = JSON.parse(value);

            value = parsed;
          } catch(err) {}

          callback(err, value);

          clientPool.release(spec.redisConfig, client);
        });
      });
    };

    getAll = function(prefix, callback) {
      clientPool.acquire(spec.redisConfig, function(err, client) {
        if (err) return callback(err);

        client.keys(prefix + '*', function(err, keys) {
          if (err) {
            callback(err);

            clientPool.release(spec.redisConfig, client);

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

              clientPool.release(spec.redisConfig, client);

              return;
            }

            var mappedValues = {};

            _.each(keys, function(key) {
              var value = values.shift();

              try {
                var parsed = JSON.parse(value);

                value = parsed;
              } catch(err) {}

              mappedValues[key.split(':').pop()] = value;
            });
            
            callback(null, mappedValues);
            
            clientPool.release(spec.redisConfig, client);
          });
        });
      });
    };
  } else {
    NeDB = NeDB || require('nedb');

    spec.nedbConfig = spec.nedbConfig || {};
    spec.nedbConfig.filename = spec.nedbConfig.filename || 'instances.db';
    spec.nedbConfig.autocompactionInterval = spec.nedbConfig.autocompactionInterval || 5000;
    if (!_.isBoolean(spec.nedbConfig.autoload)) spec.nedbConfig.autoload = true;

    var nedb = new NeDB(spec.nedbConfig);
    nedb.persistence.setAutocompactionInterval(spec.nedbConfig.autocompactionInterval);

    var wrap = function(value) {
      if (!_.isPlainObject(value)) return { _nedb_wrapped: value };
      else return value;
    };

    var unwrap = function(value) {
      return value._nedb_wrapped || value;
    }

    set = function(key, value, callback) {
      value = wrap(value);

      nedb.findOne({ _id: key }, function(err, existingValue) {
        if (err) return callback(err);

        value._id = key;

        if (existingValue) {
          nedb.update({ _id: key }, value, {}, function(err) {
            delete value._id;

            callback(err);
          });
        } else {
          nedb.insert(value, function(err) {
            delete value._id;

            callback(err);
          });
        }
      });
    };

    remove = function(key, callback) {
      nedb.remove({ _id: key }, {}, callback);
    };

    removeAll = function(prefix, callback) {
      var re = new RegExp('^' + prefix);

      nedb.remove({ _id: re }, { multi: true }, callback);
    };

    list = function(prefix, callback) {
      var re = new RegExp('^' + prefix);

      nedb.find({ _id: re }, function(err, values) {
        if (err) return callback(err);

        var list = [];

        _.each(values, function(value) {
          list.push(value._id.split(':').pop());
        });

        callback(null, list);
      });
    };

    get = function(key, callback) {
      nedb.findOne({ _id: key }, function(err, value) {
        if (err) return callback(err);

        if (value) {
          delete value._id;

          value = unwrap(value);
        }

        callback(null, value);
      });
    };

    getAll = function(prefix, callback) {
      var re = new RegExp('^' + prefix);

      //TODO: filter by status here already (efficiency)
      nedb.find({ _id: re }, function(err, values) {
        if (err) return callback(err);

        var mappedValues = {};

        _.each(values, function(value) {
          mappedValues[value._id.split(':').pop()] = unwrap(value);

          delete value._id;
        });

        callback(null, mappedValues);
      });
    };
  }



  var instances = {
    set: function(args, callback) {
      if (validateInstanceArgs(args, callback) !== null) return;

      if (!args.instance) return callback(new Error('instance must be specified'));

      var params = args.instance.parameters;
      delete args.instance.parameters;

      var res = args.instance.results;
      delete args.instance.results;

      async.series([
        function(callback) {
          set(getInstancePrefix(args) + args.id, args.instance, callback);
        },
        function(callback) {
          if (!params) return callback();

          async.eachSeries(_.keys(params), function(name, callback) {
            var value = params[name];

            if (!value) return callback();
            
            args.name = name;
            args.value = value;

            parameters.set(args, callback);
          }, callback);
        },
        function(callback) {
          if (!res) return callback();

          async.eachSeries(_.keys(res), function(name, callback) {
            var value = res[name];

            if (!value) return callback();
            
            args.name = name;
            args.value = value;

            results.set(args, callback);
          }, callback);
        }
      ], callback);
    },
    get: function(args, callback) {
      if (validateInstanceArgs(args, callback) !== null) return;

      var instance;

      async.series([
        function(callback) {
          get(getInstancePrefix(args) + args.id, function(err, inst) {
            instance = inst;

            callback(err);
          });
        },
        function(callback) {
          if (!instance) return callback();

          parameters.list(args, function(err, list) {
            instance.parameters_list = list || [];

            callback(err);
          });
        },
        function(callback) {
          if (!instance) return callback();

          results.list(args, function(err, list) {
            instance.results_list = list || [];

            callback(err);
          });
        },
        function(callback) {
          if (!instance || !args.embedParameters) return callback();

          if (args.embedParameters !== 'all' && _.isArray(args.embedParameters)) {
            args.filter = args.embedParameters;
          }

          parameters.getAll(args, function(err, params) {
            instance.parameters = params;

            delete args.filter;

            callback(err);
          });
        },
        function(callback) {
          if (!instance || !args.embedResults) return callback();

          if (args.embedResults !== 'all' && _.isArray(args.embedResults)) {
            args.filter = args.embedResults;
          }

          results.getAll(args, function(err, results) {
            instance.results = results;

            delete args.filter;

            callback(err);
          });
        }
      ], function(err) {
        callback(err, instance);
      });
    },
    getAll: function(args, callback) {
      args = args || {};
      args.id = '*';

      if (validateInstanceArgs(args, callback) !== null) return;

      getAll(getInstancePrefix(args), function(err, instances) {
        if (err) return callback(err);

        if (args.status) {
          _.each(instances, function(instance, id) {
            if (instance.status !== args.status) {
              delete instances[id];
            }
          });
        }

        callback(null, instances);
      });
    },
    remove: function(args, callback) {
      if (validateInstanceArgs(args, callback) !== null) return;

      removeAll(getParameterPrefix(args), function(err) {
        if (err) return callback(err);

        removeAll(getResultPrefix(args), function(err) {
          if (err) return callback(err);

          remove(getInstancePrefix(args) + args.id, callback);
        });
      });
    }
  };

  var parameters = {
    set: function(args, callback) {
      if (validateParameterArgs(args, callback) !== null) return;

      if (!args.value) return callback(new Error('value must be specified'));

      set(getParameterPrefix(args) + args.name, args.value, callback);
    },
    list: function(args, callback) {
      args.name = '*';

      if (validateParameterArgs(args, callback) !== null) return;

      list(getParameterPrefix(args), callback);
    },
    get: function(args, callback) {
      if (validateParameterArgs(args, callback) !== null) return;

      get(getParameterPrefix(args) + args.name, callback);
    },
    getAll: function(args, callback) {
      args.name = '*';

      if (validateParameterArgs(args, callback) !== null) return;

      getAll(getParameterPrefix(args), function(err, params) {
        if (err || !args.filter) return callback(err, params);

        filtered = {};

        _.each(filter, function(paramName) {
          if (params[paramName]) filtered[paramName] = params[paramName];
        });

        callback(null, filtered);
      });
    },
    remove: function(args, callback) {
      if (validateParameterArgs(args, callback) !== null) return;

      remove(getParameterPrefix(args) + args.name, callback);
    }
  };

  var results = {
    set: function(args, callback) {
      if (validateResultArgs(args, callback) !== null) return;

      if (!args.value) return callback(new Error('value must be specified'));

      set(getResultPrefix(args) + args.name, args.value, callback);
    },
    list: function(args, callback) {
      args.name = '*';

      if (validateResultArgs(args, callback) !== null) return;

      list(getResultPrefix(args), callback);
    },
    get: function(args, callback) {
      if (validateResultArgs(args, callback) !== null) return;

      get(getResultPrefix(args) + args.name, callback);
    },
    getAll: function(args, callback) {
      args.name = '*';

      if (validateResultArgs(args, callback) !== null) return;

      getAll(getResultPrefix(args), function(err, res) {
        if (err || !args.filter) return callback(err, res);

        filtered = {};

        _.each(filter, function(resName) {
          if (res[resName]) filtered[resName] = res[resName];
        });

        callback(null, filtered);
      });
    },
    remove: function(args, callback) {
      if (validateResultArgs(args, callback) !== null) return;

      remove(getResultPrefix(args) + args.name, callback);
    }
  };



  // Helper funtions
  var validateInstanceArgs = function(args, callback) {
    var err = null;

    if (!args) {
      err = new Error('arguments must not be null or undefined');
    } else if ((!args.id && !args.instance) || (!args.id && args.instance && !args.instance.id)) {
      err = new Error('instance id must be specified');
    } else if (!args.executableName && !args.invokerName) {
      err = new Error('either executable name or invoker name must be specified');
    }

    if (args.instance) {
      args.id = args.id || args.instance.id;
      args.instance.id = args.id;
    }

    if (err && callback) callback(err);

    return err;
  };

  var getCommonPrefixPart = function(args) {
    var prefix = '';

    if (args.executableName) prefix += 'executable:' + args.executableName + ':';
    else if (args.invokerName) prefix += 'invoker:' + args.invokerName + ':';

    return prefix;
  };

  var getInstancePrefix = function(args) {
    return spec.scope + ':instance:' + getCommonPrefixPart(args);
  };

  var validateParameterArgs = function(args, callback) {
    var err = validateInstanceArgs(args, callback);

    if (err !== null) {
      return err;
    } else if (!args.name) {
      err = new Error('parameter or result name must be specified');
    }

    if (err && callback) callback(err);

    return err;
  };

  var getParameterPrefix = function(args) {
    return spec.scope + ':parameter:' + getCommonPrefixPart(args) + args.id + ':';
  };

  var validateResultArgs = validateParameterArgs;

  var getResultPrefix = function(args) {
    return spec.scope + ':result:' + getCommonPrefixPart(args) + args.id + ':';
  };



  obj.instances = instances;
  obj.parameters = parameters;
  obj.results = results;

  return obj;
};
