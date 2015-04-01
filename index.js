var debug = require('debug')(require('./package.json').name);
var _ = require('lodash');
var async = require('async');
var RedisBackend = require('./lib/Redis');
var NeDBBackend = require('./lib/NeDB');



module.exports = function(spec) {
  var obj = {};

  spec = spec || {};
  spec.scope = spec.scope || 'default';
  spec.forceRedis = spec.forceRedis || false;

  var apiSpec = spec.apiSpec || { executables: {}, invokers: {} };

  var backend;

  if (spec.redisConfig || spec.forceRedis) {
    backend = RedisBackend(spec.redisConfig);
  } else {
    backend = NeDBBackend(spec.nedbConfig);
  }



  //
  // args:
  //   id (string)
  //   instance (object)
  //   parameterName (string)
  //   resultName (string)
  //   value (string)
  //   executableName (string)
  //   invokerName (string)
  //   status (string)
  //   embedParameters ('all' || array)
  //   embedResults ('all' || array)
  //   preferBase64 (boolean)
  //

  // Common DB access functions
  var instances = {
    set: function(args, callback) {
      if (prepareInstanceArgs(args, callback) !== null) return;

      if (!args.instance) return callback(new Error('instance must be specified'));

      var instance = _.clone(args.instance);

      var params = instance.parameters;
      delete instance.parameters;

      var res = instance.results;
      delete instance.results;

      async.series([
        function(callback) {
          backend.set(getInstancePrefix(args) + args.id, instance, 'json_object', callback);
        },
        function(callback) {
          if (!params) return callback();

          async.eachSeries(_.keys(params), function(name, callback) {
            var value = params[name];

            if (!value) return callback();
            
            args.parameterName = name;
            args.value = value;

            parameters.set(args, callback);
          }, callback);
        },
        function(callback) {
          if (!res) return callback();

          async.eachSeries(_.keys(res), function(name, callback) {
            var value = res[name];

            if (!value) return callback();
            
            args.resultName = name;
            args.value = value;

            results.set(args, callback);
          }, callback);
        }
      ], callback);
    },
    get: function(args, callback) {
      if (prepareInstanceArgs(args, callback) !== null) return;

      var instance;

      async.series([
        function(callback) {
          backend.get(getInstancePrefix(args) + args.id, 'json_object', function(err, inst) {
            instance = inst;

            callback(err);
          });
        },
        function(callback) {
          if (!instance) return callback();

          parameters.list(args, function(err, list) {
            instance.parameters_stored = list || [];

            callback(err);
          });
        },
        function(callback) {
          if (!instance) return callback();

          results.list(args, function(err, list) {
            instance.results_stored = list || [];

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
      args.id = '*';

      if (prepareInstanceArgs(args, callback) !== null) return;

      backend.getAll(getInstancePrefix(args), 'json_object', function(err, instances) {
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
      if (prepareInstanceArgs(args, callback) !== null) return;

      backend.removeAll(getParameterPrefix(args), function(err) {
        if (err) return callback(err);

        backend.removeAll(getResultPrefix(args), function(err) {
          if (err) return callback(err);

          backend.remove(getInstancePrefix(args) + args.id, callback);
        });
      });
    }
  };

  var parameters = {
    set: function(args, callback) {
      if (prepareParameterArgs(args, callback) !== null) return;

      if (!args.value) return callback(new Error('value must be specified'));

      backend.set(getParameterPrefix(args) + args.parameterName, args.value, getType(args), callback);
    },
    list: function(args, callback) {
      args.parameterName = '*';

      if (prepareParameterArgs(args, callback) !== null) return;

      backend.list(getParameterPrefix(args), callback);
    },
    get: function(args, callback) {
      if (prepareParameterArgs(args, callback) !== null) return;

      backend.get(getParameterPrefix(args) + args.parameterName, getType(args), callback);
    },
    getAll: function(args, callback) {
      args.parameterName = '*';

      if (prepareParameterArgs(args, callback) !== null) return;

      backend.getAll(getParameterPrefix(args), getType(args), function(err, params) {
        if (err || !args.filter) return callback(err, params);

        filtered = {};

        _.each(args.filter, function(paramName) {
          if (params[paramName]) filtered[paramName] = params[paramName];
        });

        callback(null, filtered);
      });
    },
    remove: function(args, callback) {
      if (prepareParameterArgs(args, callback) !== null) return;

      backend.remove(getParameterPrefix(args) + args.parameterName, callback);
    }
  };

  var results = {
    set: function(args, callback) {
      if (prepareResultArgs(args, callback) !== null) return;

      if (!args.value) return callback(new Error('value must be specified'));

      backend.set(getResultPrefix(args) + args.resultName, args.value, getType(args), callback);
    },
    list: function(args, callback) {
      args.resultName = '*';

      if (prepareResultArgs(args, callback) !== null) return;

      backend.list(getResultPrefix(args), callback);
    },
    get: function(args, callback) {
      if (prepareResultArgs(args, callback) !== null) return;

      backend.get(getResultPrefix(args) + args.resultName, getType(args), callback);
    },
    getAll: function(args, callback) {
      args.resultName = '*';

      if (prepareResultArgs(args, callback) !== null) return;

      backend.getAll(getResultPrefix(args), getType(args), function(err, res) {
        if (err || !args.filter) return callback(err, res);

        filtered = {};

        _.each(args.filter, function(resName) {
          if (res[resName]) filtered[resName] = res[resName];
        });

        callback(null, filtered);
      });
    },
    remove: function(args, callback) {
      if (prepareResultArgs(args, callback) !== null) return;

      backend.remove(getResultPrefix(args) + args.resultName, callback);
    }
  };



  // Helper funtions
  var getType = function(args) {
    var defaultType = 'text_string';
    var type = defaultType;

    var item = apiSpec.executables[args.executableName] || apiSpec.invokers[args.invokerName];

    if (item) {
      if (args.parameterName === '*') {
        type = {};

        _.each(item.parameters_schema, function(param, name) {
          type[name] = param.type || defaultType;
        });
      } else if (args.resultName === '*') {
        type = {};

        _.each(item.results_schema, function(result, name) {
          type[name] = result.type || defaultType;
        });
      } else if (item.parameters_schema[args.parameterName]) {
        type = item.parameters_schema[args.parameterName].type || defaultType;
      } else if (item.results_schema[args.resultName]) {
        type = item.results_schema[args.resultName].type || defaultType;
      }
    }

    if (type === 'string') {
      type = 'text_string';
    } else if (type === 'byte_string' && args.preferBase64) {
      type = '__base64_string';
    } else if (_.isPlainObject(type) && args.preferBase64) {
      _.each(type, function(t, name) {
        if (t === 'string') type[name] = 'text_string';
        else if (t === 'byte_string') type[name] = '__base64_string';
      });
    }

    return type;
  };

  var prepareInstanceArgs = function(args, callback) {
    var err = null;

    if (!args) {
      err = new Error('arguments must not be null or undefined');
    } else if ((!args.id && !args.instance) || (!args.id && args.instance && !args.instance.id)) {
      err = new Error('instance id must be specified');
    } else if (!args.executableName && !args.invokerName) {
      err = new Error('either executable name or invoker name must be specified');
    }

    if (err && callback) callback(err);

    if (args.instance) {
      args.id = args.id || args.instance.id;
      args.instance.id = args.id;
    }

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

  var prepareParameterArgs = function(args, callback) {
    var err = prepareInstanceArgs(args, callback);

    delete args.resultName;

    if (err !== null) {
      return err;
    } else if (!args.parameterName) {
      err = new Error('parameter name must be specified');
    }

    if (err && callback) callback(err);

    return err;
  };

  var getParameterPrefix = function(args) {
    return spec.scope + ':parameter:' + getCommonPrefixPart(args) + args.id + ':';
  };

  var prepareResultArgs = function(args, callback) {
    var err = prepareInstanceArgs(args, callback);

    delete args.parameterName;

    if (err !== null) {
      return err;
    } else if (!args.resultName) {
      err = new Error('result name must be specified');
    }

    if (err && callback) callback(err);

    return err;
  };

  var getResultPrefix = function(args) {
    return spec.scope + ':result:' + getCommonPrefixPart(args) + args.id + ':';
  };



  obj.instances = instances;
  obj.parameters = parameters;
  obj.results = results;

  return obj;
};
