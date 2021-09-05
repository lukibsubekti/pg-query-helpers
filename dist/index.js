const { Pool, Client } = require("pg");
const { v4: uuidv4 } = require('uuid');
var SqlString = require('sqlstring');
const debug = require('debug')('pg-query-helpers:index');

function getQueryParams(data) {
  let queryFields = [];
  let queryValues = [];
  let values = [];
  let counter = 1;

  for (let prop in data) {
    if (data.hasOwnProperty(prop)) {
      queryFields.push(prop);
      queryValues.push(`$${counter}`);
      values.push(data[prop]);
      counter++;
    }
  }

  return {
    strFields: queryFields.join(', '), 
    strValues: queryValues.join(', '),
    values
  }
}

function getUpdateQueryParams(data) {
  let querySets = [];
  let values = [];
  let counter = 1;

  for (let prop in data) {
    if (data.hasOwnProperty(prop)) {
      querySets.push(`${prop} = $${counter}`);
      values.push(data[prop]);
      counter++;
    }
  }

  return {
    strSets: querySets.join(', '),
    values
  }
}

function getQueryError(statusCode=500, message='Database error') {
  let err = new Error(message);
  err.statusCode = statusCode;
  return err;
}

function sqlEscape(value) {
  if (value==='NULL' || value===null) {
    return 'NULL';
  } else if (/^\%.*\%$/.test(value)) {
    return "'" + value + "'";
  } else {
    return SqlString.escape(value);
  }
}

function getWhereQuery(data, comp='AND') {

  if (Array.isArray(data)) {
    let strArr = [];
    data.forEach((val, idx) => {
      strArr.push(getWhereQuery(val));
    });
    return '(' + strArr.join(` ${comp} `) + ')';

  } else if (typeof data === 'object') {
    let strArr = [];

    for (let prop in data) {
      if (data.hasOwnProperty(prop)) {

        if (typeof data[prop] !== 'object') {
          strArr.push(`${prop} = ${sqlEscape(data[prop])}`);
        } else {

          if (prop === '$and') {
            strArr.push(getWhereQuery(data.$and, 'AND'));

          } 
          else if (prop === '$or') {
            strArr.push(getWhereQuery(data.$or, 'OR'));

          } else {
            strArr.push(`${prop} ${data[prop].op || '='} ${sqlEscape(data[prop].value)}`);
          }
        }
      }
    }

    return '(' + strArr.join(` ${comp} `) + ')';
  } else if (typeof data === 'string') {
    return data;
  } else {
    return '';
  }
}

function getJoinSingle(join) {
  if (typeof join === 'string') {
      return ' ' + join + ' ';
  }
  else if (typeof join === 'object') {
      return ` ${join.type} JOIN ${join.table} ON ${join.condition} `;
  }
  return false;
}

/**
 * 
 * @param { {type: 'left' | 'right' | 'inner' | 'full outer', table: string, condition: string} |
 *  {type: 'left' | 'right' | 'inner' | 'full outer', table: string, condition: string}[] } join 
 * @returns 
 */
function getJoin(join) {
  if (Array.isArray(join) && join.length) {
      let joinParts = [];
      join.forEach((v) => {
          let part = getJoinSingle(v);
          if (part) {
              joinParts.push(part);
          }
      });
      return joinParts.join('');
  }
  else if (!Array.isArray(join)) {
      return getJoinSingle(join);
  }
  else {
      return false;
  }
}

class Model {
  constructor(client, table='') {
    this.client = client;
    this.table = table;
    this.isValidModel();
  }

  isValidModel() {
    if ( 
      ((this.client instanceof Pool) === false && (client instanceof Client) === false) 
      || typeof this.table !== 'string'
    ) {
      throw new Error('Invalid constructor parameters for Model');
    }
  }

  getMany(filter, options={}) {

    return new Promise((resolve, reject) => {
      // check options
      let select = '*';
      if (options.select) {
        select = options.select;
      }
      let join = false;
      if (options.join) {
        join = getJoin(options.join);
      }

      let whereStr = getWhereQuery(filter);
      const queryText = `SELECT ${select} FROM ${this.table} ${join ? join : ''} ${whereStr && whereStr!='()' ? 'WHERE ' + whereStr : ''}`;

      this.client
        .query({
          text: queryText
        })
        .then((result) => {
          if (result.rows.length > 0) {
            resolve(result.rows);
          } else {
            resolve([]);
          }
        })
        .catch(reason => {
          debug(`Read rejection: ${reason}`);
          debug(`Query: ${queryText}`);
          reject(getQueryError(500, 'Request cannot be completed.'));
        })
    });
  }

  getOne(filter, options={failOnEmpty:true}) {

    return new Promise((resolve, reject) => {
      let whereStr = getWhereQuery(filter);
      const queryText = `SELECT * FROM ${this.table} ${whereStr ? 'WHERE ' + whereStr : ''} LIMIT 1`;

      this.client
        .query({
          text: queryText
        })
        .then((result) => {
          if (result.rows.length > 0) {
            resolve(result.rows[0]);
          } else if (options.failOnEmpty===false) {
            resolve(null);
          } else {
            reject(getQueryError(404, 'Record cannot be found.'));
          }
        })
        .catch(reason => {
          debug(`Read rejection: ${reason}`);
          debug(`Query: ${queryText}`);
          reject(getQueryError(500, 'Request cannot be completed.'));
        })
    });
  }

  /**
   * 
   * @param {string} id 
   * @returns {any}
   */
  getOneById(id, options={failOnEmpty:true}) {
    return this.getOne({id}, options);
  }

  /**
   * 
   * @param {{[props:string]: any}} data 
   * @returns {{id:string}}
   */
  insertOne(data) {
    let id = uuidv4();
    data.id = id;
    let {strFields, strValues, values} = getQueryParams(data);

    return new Promise((resolve, reject) => {
      const queryText = `INSERT INTO ${this.table}(${strFields}) VALUES (${strValues})`;

      this.client
        .query({
          text: queryText,
          values: values
        })
        .then(result => {
          debug(`rowCount: ${result.rowCount}`);

          if (result.rowCount > 0) {
            resolve(data.id)
          } else {
            reject(getQueryError(500, 'Failed to insert new record.'));
          }
        })
        .catch(reason => {
          debug(`Insert rejection: ${reason}`);
          debug(`Query: ${queryText}`);
          reject(getQueryError(500, 'Failed to insert new record.'));
        });
    })
  }

  updateOne(id, data) {
    return new Promise((resolve, reject) => {
      const {strSets, values} = getUpdateQueryParams(data);
      const whereStr = getWhereQuery({id});
      const queryText = `UPDATE ${this.table} SET ${strSets} ${whereStr ? 'WHERE ' + whereStr : ''}`;

      this.client
        .query({
          text: queryText,
          values: values
        })
        .then(result => {
          debug(`rowCount: ${result.rowCount}`);

          if (result.rowCount > 0) {
            resolve(true)
          } else {
            reject(getQueryError(500, 'Failed to update the record.'));
          }
        })
        .catch(reason => {
          debug(`Update rejection: ${reason}`);
          debug(`Query: ${queryText}`);
          reject(getQueryError(500, 'Failed to update the record.'));
        });
    })
  }

  deleteOne(filter) {
    return new Promise((resolve, reject) => {
      let whereStr = getWhereQuery(filter);
      const queryText = `DELETE FROM ${this.table} ${whereStr ? 'WHERE ' + whereStr : ''}`;

      this.client
        .query({
          text: queryText
        })
        .then((result) => {
          resolve(true);
        })
        .catch(reason => {
          debug(`Delete rejection: ${reason}`);
          debug(`Query: ${queryText}`);
          reject(getQueryError(500, 'Request cannot be completed.'));
        })
    });
  }

  deleteOneById(id) {
    return this.deleteOne({id});
  }

  runTransactions(queries) {
    if ( !(this.client instanceof Pool) ) {
      return Promise.reject(getQueryError(500, 'Client shuold be a Pool.'));
    }

    let promiseChain = Promise.resolve([]);
    let currentClient = null;
    let state = '';
    let results = [];

    return this.client.connect()
      .then(client => {
        currentClient = client;
        state = 'BEGIN';
        return currentClient.query(state)
      })
      .then(_ => {
        state = 'RUN';

        for (let i=0; i<queries.length; i++) {
          promiseChain = promiseChain.then(chainResult => {
            return currentClient.query(queries[i]).then(queryResult => {
              return [...chainResult, queryResult];
            })
          })
        }

        return promiseChain;
      })
      .then(chainResult => {
        results = chainResult;
        state = 'COMMIT';
        return currentClient.query(state)
      })
      .then(_ => {
        currentClient.release();
        debug(`Client connection is released.`);
        return Promise.resolve(results);
      })
      .catch(errTransaction => {
        debug(`Transaction error: ${errTransaction}`);

        if (currentClient) { // client is established
          if (state === 'BEGIN') { // if just starting (begin query failed)
            currentClient.release();
            debug(`Client connection is released.`);
            return Promise.reject(getQueryError(500, 'Failed in starting the transaction.'));

          } else { // already started (any transaction query failed)
            return currentClient
              .query('ROLLBACK')
              .then(_ => { // success rollback query --> keep throw rejection to indicates transaction actually failed
                debug(`Rollback success`);
                return Promise.reject(getQueryError(500, 'Transaction error but it has been rolled back.'));
              })
              .catch(errRollback => {
                currentClient.release();
                debug(`Client connection is released.`);
                
                if (errRollback.statusCode) { // comes from success rollback query
                  return Promise.reject(errRollback);
                } else { // comes from error rollback query
                  debug(`Rollback error: ${errRollback}`);
                  return Promise.reject(getQueryError(500, 'Transaction error and it failed to be rolled back.'));
                }
              });

          }
        } else { // no available client
          return Promise.reject(getQueryError(500, 'No available client in the Pool.'));
        }
      })
  }
}

module.exports = { 
  Model, 
  getQueryParams, 
  getUpdateQueryParams,
  getWhereQuery,
  getJoin 
};