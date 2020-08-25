const express = require('express');
const swaggerJsdoc = require('swagger-jsdoc');
const swaggerUi = require('swagger-ui-express');

// router for swagger requests
const swaggerRouter = express.Router();

const options = {
  apis: ['./routes/*.js'], // painfully, this path is relative to the app.js entrypoint, not relative to this file.
  definition: {
    info: {
      openapi: '3.0.0',
      description: 'Structure API',
      swagger: '3.0',
      title: 'Structure API',
      version: '0.0.0',
    },
  },
};

// setup swagger ui route
const swaggerSpecs = swaggerJsdoc(options);
swaggerRouter.use('/', swaggerUi.serve, swaggerUi.setup(swaggerSpecs));

module.exports = swaggerRouter;
