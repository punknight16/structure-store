var express = require('express');
var session = require("express-session")
var bodyParser = require('body-parser')
var flash = require('connect-flash');
var bcrypt   = require('bcrypt-nodejs');
var mje = require('mongo-json-escape')
const mongo = require('mongodb').MongoClient
const mongoObjectID = require('mongodb').ObjectID
const url = 'mongodb://localhost:27017/'
var snowflake = require('snowflake-sdk');

var rp = require('request-promise-native');

var textParser = bodyParser.text({limit: '50mb', extended: true})
var jsonParser = bodyParser.json({limit: '50mb', extended: true})
// configure app to use bodyParser()
// this will let us get the data from a POST
var router = express.Router();
router.use(flash())
module.exports = router;
