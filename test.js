var Cassandra = require('./cassandra');
var config = { keyspace: 'scopradb'};
var cassandra = new Cassandra(config);
console.log("Cassandra - > ", cassandra);
