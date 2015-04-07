var NeDB = require('nedb');
var _ = require('lodash');



module.exports = function(spec) {
  var obj = {};

  spec = spec || {};

  spec.filename = spec.filename || 'instances.db';
  spec.autocompactionInterval = spec.autocompactionInterval || 5000;
  if (!_.isBoolean(spec.autoload)) spec.autoload = true;

  var nedb = new NeDB(spec);
  nedb.persistence.setAutocompactionInterval(spec.autocompactionInterval);

  var wrap = function(value, type) {
    if (Buffer.isBuffer(value)) { // && type === 'binary'
      value = value.toString('base64'); //value.toJSON()
    }

    value = { data: value };

    return value;
  };

  var unwrap = function(value, type) {
    if (!value || !value.data) return null;

    value = value.data;

    if (type === 'binary') {
      value = new Buffer(value, 'base64');
    }

    return value;
  };

  var set = function(key, value, type, callback) {
    value = wrap(value, type);

    nedb.findOne({ _id: key }, function(err, existingValue) {
      if (err) return callback(err);

      value._id = key;

      if (existingValue) {
        nedb.update({ _id: key }, value, {}, callback);
      } else {
        nedb.insert(value, callback);
      }
    });
  };

  var remove = function(key, callback) {
    nedb.remove({ _id: key }, {}, callback);
  };

  var removeAll = function(prefix, callback) {
    var re = new RegExp('^' + prefix);

    nedb.remove({ _id: re }, { multi: true }, callback);
  };

  var list = function(prefix, callback) {
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

  var get = function(key, type, callback) {
    nedb.findOne({ _id: key }, function(err, value) {
      if (err) return callback(err);

      value = unwrap(value, type);

      callback(null, value);
    });
  };

  var getAll = function(prefix, type, callback) {
    var re = new RegExp('^' + prefix);

    //TODO: filter by status here already (efficiency)
    nedb.find({ _id: re }, function(err, values) {
      if (err) return callback(err);

      var mappedValues = {};

      _.each(values, function(value) {
        var name = value._id.split(':').pop();

        var t = type;
        if (_.isPlainObject(type)) t = type[name];

        mappedValues[name] = unwrap(value, t);
      });

      callback(null, mappedValues);
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
