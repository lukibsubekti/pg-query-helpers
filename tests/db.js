const assert = require('assert');
const {Pool} = require('pg');
const config = require('../src/config');

const { Model } =  require('../src/index');

const pgpool = new Pool({
  user: config.postgre.user,
  host: config.postgre.host,
  database: config.postgre.db,
  password: config.postgre.password,
  port: config.postgre.port
});

const unit = {};

unit['runTransactions should return array'] = function(done){
  const model = new Model(pgpool, '');
  const queries = [
    `UPDATE users SET balance = balance - 1 WHERE id = 1`,
    `UPDATE items SET stock = stock - 2, update_on = 1000 WHERE id = 1`,
  ];
  model.runTransactions(queries)
    .then(results => {
      console.log(results);
      assert.strictEqual(Array.isArray(results), true);
      done();
    })
    .catch(reason => {
      console.log(reason);
      done();
    })
};

module.exports = unit;