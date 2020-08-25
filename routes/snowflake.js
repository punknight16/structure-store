const express = require('express');
const {promisify} = require("es6-promisify");
const snowflake = require('snowflake-sdk');
const util = require('util');

// router with all snowflake handlers
const snowflakeRouter = express.Router();

// validation function for snowflake connection parameters
function validateSnowflakeConnectionParams(account, username, password) {
  if (!account) {
    return { 'status': 'unsuccessful', 'reason': 'invalid account' };
  } else if (!username) {
    return { 'status': 'unsuccessful', 'reason': 'invalid username' };
  } else if (!password) {
    return { 'status': 'unsuccessful', 'reason': 'invalid password' };
  }
}

// snowflake connection wrapper to handle optional parameters
function getSnowflakeConnection(account, username, password, role, warehouse) {
  // add required params
  let connParams = {
    account: account,
    username: username,
    password: password
  };

  // add optional params
  if (role) {
    connParams.role = role;
  }
  if (warehouse) {
    connParams.warehouse = warehouse;
  }

  // return connection
  const sfConn = snowflake.createConnection(connParams);
  return sfConn;
}

/**
 * Async wrapper for executing snowflake queries
 * 
 * Code provided from example in snowflake sdk github issue below:
 *  https://github.com/snowflakedb/snowflake-connector-nodejs/issues/3
 */
async function executeSnowflakeQuery(sfConn, options) {
  return new Promise((resolve, reject) => {
    sfConn.execute({
      ...options,
      complete: function(err, stmt, rows) {
        if (err) {
          reject(err)
        } else {
          resolve({stmt, rows})
        }
      }
    })
  });
}

/**
 * @swagger
 * /snowflake:
 *   post:
 *     summary: Create and validate a Snowflake connection.
 *     tags:
 *       - Snowflake
 *     parameters:
 *       - name: account
 *         description: Account url prefix for target Snowflake environment (does not include ".snowflakecomputing.com").
 *         in: body
 *         required: true
 *         type: string
 *         example: my_snowflake_account
 *       - name: username
 *         description: Username to use for Snowflake auth.
 *         in: body
 *         required: true
 *         type: string
 *         example: some.user@company.com
 *       - name: password
 *         description: Password to use for Snowflake auth.
 *         in: body
 *         required: true
 *         type: string
 *         example: secret_password
 *       - name: warehouse
 *         description: Snowflake Warehouse to use to execute future queries. If not provided, the user's default warehouse is used.
 *         in: body
 *         required: false
 *         type: string
 *         example: DEMO_WH
 *       - name: role
 *         description: Snowflake Role to use to execute future queries. If not provided, the user's default role is used.
 *         in: body
 *         required: false
 *         type: string
 *         example: ACCOUNTADMIN
 *     responses: 
 *       200:
 *         description: status and Snowflake connection id.
 */
snowflakeRouter.post('/', async function (req, res, next) {
  // get body params
  const account   = req.body.account;
  const username  = req.body.username;
  const password  = req.body.password;
  const role      = req.body.role; // not required
  const warehouse = req.body.warehouse; // not requireds

  // validate config
  const configValidation = validateSnowflakeConnectionParams(account, username, password);
  if (configValidation) {
    res.send(configValidation);
  }

  // Connect
  const sfConn = getSnowflakeConnection(account, username, password, role, warehouse);
  try {
    await util.promisify(sfConn.connect)();
  } catch (err) {
    const errMsg = "Error connecting to Snowflake: " + err.message;
    console.error(errMsg);
    res.send({'status': 'unsuccessful', 'reason': errMsg});
  }

  // return connection ID as confirmation of status
  console.debug('Successfully connected to Snowflake');
  res.send({ 'status': 'successful', 'connectionId': sfConn.getId() });
});

/**
 * @swagger
 * /snowflake/query:
 *   post:
 *     summary: Create and execute a Snowflake query.
 *     tags:
 *       - Snowflake
 *     parameters:
 *       - name: account
 *         description: Account url prefix for target Snowflake environment (does not include ".snowflakecomputing.com").
 *         in: body
 *         required: true
 *         type: string
 *         example: my_snowflake_account
 *       - name: username
 *         description: Username to use for Snowflake auth.
 *         in: body
 *         required: true
 *         type: string
 *         example: some.user@company.com
 *       - name: password
 *         description: Password to use for Snowflake auth.
 *         in: body
 *         required: true
 *         type: string
 *         example: secret_password
 *       - name: query
 *         description: Single Snowflake query to execute. Must contain a trailing ';' semicolon.
 *         in: body
 *         required: true
 *         type: string
 *         example: SHOW DATABASES;
 *       - name: warehouse
 *         description: Snowflake Warehouse to use to execute this query. If not provided, the user's default warehouse is used.
 *         in: body
 *         required: false
 *         type: string
 *         example: DEMO_WH
 *       - name: role
 *         description: Snowflake Role to use to execute this query. If not provided, the user's default role is used.
 *         in: body
 *         required: false
 *         type: string
 *         example: ACCOUNTADMIN
 *     responses: 
 *       200:
 *         description: status and query response.
 */
snowflakeRouter.post('/query', async function (req, res, next) {
  // get body params
  const account   = req.body.account; // TODO: these are required until connection details are persisted
  const username  = req.body.username; // TODO: these are required until connection details are persisted
  const password  = req.body.password; // TODO: these are required until connection details are persisted
  const role      = req.body.role; // not required
  const warehouse = req.body.warehouse; // not required
  const query     = req.body.query;

  // validate config
  const configValidation = validateSnowflakeConnectionParams(account, username, password);
  if (configValidation) {
    res.send(configValidation);
  }
  
  // validate query text
  const queryValidation = query ? null : {'status': 'unsuccessful', 'reason': 'A query must be provided'};
  if (queryValidation) {
    res.send(queryValidation);
  }
  
  // Connect
  const sfConn = getSnowflakeConnection(account, username, password, role, warehouse);

  // Execute query
  let queryResponse = null;
  try {
    await util.promisify(sfConn.connect)();
    queryResponse = await executeSnowflakeQuery(sfConn, {sqlText: query});
  } catch (err) {
    const errMsg = "Error executing Snowflake query: " + err.message;
    console.error(errMsg);
    res.send({'status': 'unsuccessful', 'reason': errMsg});
    return;
  }

  // return connection ID as confirmation of status
  console.debug('Successfully queried Snowflake');
  res.send({ 'status': 'successful', 'response': queryResponse });
});

/**
 * @swagger
 * /snowflake/relation-preview:
 *   post:
 *     summary: Create and retrieve a relation preview. Returns the top 30 rows of a 
 *     tags:
 *       - Snowflake
 *     parameters:
 *       - name: account
 *         description: Account url prefix for target Snowflake environment (does not include ".snowflakecomputing.com").
 *         in: body
 *         required: true
 *         type: string
 *         example: my_snowflake_account
 *       - name: username
 *         description: Username to use for Snowflake auth.
 *         in: body
 *         required: true
 *         type: string
 *         example: some.user@company.com
 *       - name: password
 *         description: Password to use for Snowflake auth.
 *         in: body
 *         required: true
 *         type: string
 *         example: secret_password
 *       - name: database
 *         description: The database where the relation to be previewed exists
 *         in: body
 *         required: true
 *         type: string
 *         example: MY_DATABASE
 *       - name: schema
 *         description: The schema where the relation to be previewed exists
 *         in: body
 *         required: true
 *         type: string
 *         example: MY_SCHEMA
 *       - name: relation
 *         description: The name of the relation to be previewed.
 *         in: body
 *         required: true
 *         type: string
 *         example: MY_TABLE
 *       - name: warehouse
 *         description: Snowflake Warehouse to use to execute this query. If not provided, the user's default warehouse is used.
 *         in: body
 *         required: false
 *         type: string
 *         example: DEMO_WH
 *       - name: role
 *         description: Snowflake Role to use to execute this query. If not provided, the user's default role is used.
 *         in: body
 *         required: false
 *         type: string
 *         example: ACCOUNTADMIN
 *     responses: 
 *       200:
 *         description: status, row preview, column details, and rowcount response.
 */
snowflakeRouter.post('/relation-preview', async function (req, res, next) {
  // set consts
  const numRowsInPreview = 100; // drop this in the future if client load becomes a concern
 
  // get body params
  const account   = req.body.account; // TODO: these are required until connection details are persisted
  const username  = req.body.username; // TODO: these are required until connection details are persisted
  const password  = req.body.password; // TODO: these are required until connection details are persisted
  const database  = req.body.database;
  const schema    = req.body.schema;
  const relation  = req.body.relation;
  const role      = req.body.role; // not required
  const warehouse = req.body.warehouse; // not required

  // validate config
  const configValidation = validateSnowflakeConnectionParams(account, username, password);
  if (configValidation) {
    res.send(configValidation);
  }
  
  // validate query text
  const queryValidation = [
    database ? null : {'status': 'unsuccessful', 'reason': 'A database must be provided'},
    schema   ? null : {'status': 'unsuccessful', 'reason': 'A schema must be provided'},
    relation ? null : {'status': 'unsuccessful', 'reason': 'A relation must be provided'},
  ].filter(x=>x); // filter out nulls
  if (queryValidation.length > 0) {
    res.send(queryValidation[0]);
  }
  
  // Connect
  const sfConn = getSnowflakeConnection(account, username, password, role, warehouse);

  // Get data preview
  let dataPreviewResponse = null;
  try {
    await util.promisify(sfConn.connect)();
    const query = `SELECT * FROM ${database}.${schema}.${relation} LIMIT ${numRowsInPreview};`; 
    dataPreviewResponse = await executeSnowflakeQuery(sfConn, {sqlText: query});
  } catch (err) {
    const errMsg = "Error executing Snowflake query: " + err.message;
    console.error(errMsg);
    res.send({'status': 'unsuccessful', 'reason': errMsg});
    return;
  }

  // Get data preview
  let columnDetailsResponse = null;
  try {
    const query = `DESCRIBE TABLE ${database}.${schema}.${relation};`; 
    columnDetailsResponse = await executeSnowflakeQuery(sfConn, {sqlText: query});
  } catch (err) {
    const errMsg = "Error executing Snowflake query: " + err.message;
    console.error(errMsg);
    res.send({'status': 'unsuccessful', 'reason': errMsg});
    return;
  }

  // Get row count
  let rowcount = null;
  try {
    const query = `SELECT COUNT(*) AS CNT FROM ${database}.${schema}.${relation};`; 
    const rowcountResponse = await executeSnowflakeQuery(sfConn, {sqlText: query});
    rowcount = rowcountResponse.rows[0].CNT; // always returns a single row
  } catch (err) {
    const errMsg = "Error executing Snowflake query: " + err.message;
    console.error(errMsg);
    res.send({'status': 'unsuccessful', 'reason': errMsg});
    return;
  }

  // return connection ID as confirmation of status
  console.debug('Successfully queried Snowflake');
  res.send({
    'status'  : 'successful', 
    'preview' : dataPreviewResponse, 
    'columns' : columnDetailsResponse,
    'rowcount': rowcount
  });
});

module.exports = snowflakeRouter;
