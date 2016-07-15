'use strict';

const cassandraDriver = require('cassandra-driver');

const Cassandra = function(config) {
     var self = this;
     self.config = config === undefined ? { contactPoints : ['127.0.0.1'] } : config;
     self.cassandraDriver = cassandraDriver;
     self.client = new cassandraDriver.Client(self.config);
};

function constructInsertParams(parameters){
    var keys = Object.keys(parameters);
    var data = [];
    var queryValue = [];
    for(let key in keys){
      let index = keys[key];
      let value = typeof parameters[index] !== 'object' ? parameters[index] : (parameters[index] || undefined);
      data.push(value);
      queryValue.push('?');
    }

    var queryParams = ' (' + keys.join(', ') + ') ';
    queryValue = queryValue.join(' ,');
    var result = { queryParams: queryParams, queryValue: queryValue, data: data };
    return result;
}

function constructUpdateParams(parameters, id){
  var keys = Object.keys(parameters);
  var data = [];
  var updateParams =[];
  for(let key in keys) {
    let index = keys[key];
    let value = typeof parameters[index] !== 'object' ? parameters[index] : (parameters[index] || undefined);
    data.push(value);
    updateParams.push((index + "=?"));
  }

  data.push(id);
  updateParams = updateParams.join(', ');
  var result = { keys: updateParams, data: data };
  return result;
}

function constructFilterParams(filters){
  var keys = Object.keys(filters);
  var result = [];
  for(let key in keys) {
    let index = keys[key];
    let value = filters[index];
    let str = (index + ' = ' + value);
    result.push(str);
  }
  return result.join(' AND ');
}

Cassandra.prototype.insert = function(tableName, parameters, insertCallback) {
  var self = this;
  var insertParams = constructInsertParams(parameters);
  const query = 'INSERT INTO '+ tableName + insertParams.queryParams + 'VALUES ('+ insertParams.queryValue +')';
  self.client.execute(query, insertParams.data, { prepare: true }, insertCallback);
};

Cassandra.prototype.update = function(tableName, keyValue, parameters, updateCallback) {
  var self = this;
  var updateParams = constructUpdateParams(parameters, keyValue.value);
  const query = 'UPDATE '+ tableName + ' SET '+ updateParams.keys +' WHERE ' + keyValue.key + '=?';
  self.client.execute(query, updateParams.data, { prepare: true }, updateCallback);
};

Cassandra.prototype.findById = function(tableName, keyValue, selectors, findCallback) {
  var self = this;
  var selectParams;
  if(selectors instanceof Array){
    selectParams = selectors.join(', ')
  } else if(typeof selectors === 'function'){
    findCallback = selectors;
    selectParams = '*';
  } else {
    throw new TypeError('Invalid arguments');
  }

  const query = 'SELECT '+ selectParams +' FROM '+ tableName +' WHERE ' + keyValue.key + '=?';
  var id = [keyValue.value];
  self.client.execute(query, id, { prepare: true }, (err, result) => {
    if(!err && result && result.rowLength > 0){
      findCallback(null, result.rows[0]);
    } else if(!err){
      findCallback('not found', null);
    } else{
      findCallback(err, null);
    }
  });
};

Cassandra.prototype.find = function(query, values, options, findCallback) {
  var self = this;
  values = values || {};
  self.client.execute(query, values, { prepare: true }, (err, result) => {
    if(!err && result && result.rowLength >= 0){
      let response = {};
      response.length = result.rowLength;
      response.data = result.rows;
      findCallback(null, response);
    } else{
      findCallback(err, null);
    }
  });
};

/**
 * Inserts record into the database
 * @param {String} CQL query string
 * @param {Array} Array of values
 * @param {queryOptions} queryOptions is an object with additional options. i.e., { prepare: true, consistency: cassandra.types.consistencies.quorum }
 */
Cassandra.prototype.query = function(cql, values, queryOptions, callback) {
    var self = this;
    self.client.execute(cql, values, queryOptions, callback);
};

Cassandra.prototype.batch = function(queries, batchCallback) {
    var self = this;
    self.client.batch(queries, { prepare: true }, function(err, res) {
      if(!err){
        batchCallback(null, "Success");
      } else {
        batchCallback(err, null);
      }
    });
};


module.exports = Cassandra;
