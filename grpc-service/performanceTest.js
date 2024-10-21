// performanceTest.js

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const performance = require('performance-now');
const fs = require('fs');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

// Path to the .proto file
const PROTO_PATH = __dirname + '/protos/prize_service.proto';

// Load the protobuf
const packageDefinition = protoLoader.loadSync(
  PROTO_PATH,
  {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
  }
);

const prizeProto = grpc.loadPackageDefinition(packageDefinition).prize;

// Create a client stub
const client = new prizeProto.PrizeService(
  process.env.GRPC_SERVER_ADDRESS || 'localhost:50051',
  grpc.credentials.createInsecure()
);

/**
 * Helper function to handle gRPC responses with delay measurement.
 * @param {Function} rpcMethod - The gRPC method to invoke.
 * @param {Object} request - The request object for the gRPC method.
 * @returns {Promise<number>} - The E2E delay in milliseconds.
 */
function measureDelay(rpcMethod, request = {}) {
  return new Promise((resolve, reject) => {
    const start = performance();
    rpcMethod(request, (error, response) => {
      const end = performance();
      if (error) {
        reject(error);
      } else {
        const delay = end - start; // in milliseconds
        resolve(delay);
      }
    });
  });
}

/**
 * Runs a specific query multiple times and records the delays.
 * @param {string} queryName - The name of the query (e.g., 'query1').
 * @param {Function} rpcMethod - The gRPC method to invoke.
 * @param {Object} request - The request object for the gRPC method.
 * @param {number} iterations - Number of times to run the query.
 * @returns {Promise<number[]>} - An array of delay measurements.
 */
async function runQuery(queryName, rpcMethod, request, iterations = 100) {
  console.log(`Starting ${iterations} iterations for ${queryName}...`);
  const delays = [];

  for (let i = 0; i < iterations; i++) {
    try {
      const delay = await measureDelay(rpcMethod, request);
      delays.push(delay);
      if ((i + 1) % 10 === 0) {
        console.log(`${queryName}: Completed ${i + 1} / ${iterations} iterations.`);
      }
    } catch (error) {
      console.error(`${queryName}: Error on iteration ${i + 1}:`, error.message);
      delays.push(null); // Indicate failed iteration
    }
  }

  // Filter out null values (failed iterations)
  const successfulDelays = delays.filter(delay => delay !== null);
  console.log(`${queryName}: Completed with ${successfulDelays.length} successful iterations.`);
  return successfulDelays;
}

/**
 * Main function to execute all queries and collect delay data.
 */
async function main() {
  const argv = yargs(hideBin(process.argv))
    .option('iterations', {
      alias: 'n',
      type: 'number',
      description: 'Number of iterations per query',
      default: 100,
    })
    .help()
    .alias('help', 'h')
    .argv;

  const iterations = argv.iterations;

  const delayResults = {};

  // Define the four queries
  const queries = [
    {
      name: 'GetPrizesByCategory',
      method: client.GetPrizesByCategory.bind(client),
      request: {} // Empty request
    },
    {
      name: 'CountLaureatesByCategoryAndYearRange',
      method: client.CountLaureatesByCategoryAndYearRange.bind(client),
      request: {
        category: 'physics',
        startYear: 2013,
        endYear: 2023
      }
    },
    {
      name: 'CountLaureatesByMotivationKeyword',
      method: client.CountLaureatesByMotivationKeyword.bind(client),
      request: {
        keyword: 'development'
      }
    },
    {
      name: 'GetLaureateDetailsByName',
      method: client.GetLaureateDetailsByName.bind(client),
      request: {
        firstname: 'Arthur',
        surname: 'Ashkin'
      }
    }
  ];

  // Execute each query
  for (const query of queries) {
    const delays = await runQuery(query.name, query.method, query.request, iterations);
    delayResults[query.name] = delays;
  }

  // Save the delay results to a JSON file
  fs.writeFileSync('delayResults.json', JSON.stringify(delayResults, null, 2));
  console.log('Delay results saved to delayResults.json');

  // Close the gRPC client
  client.close();
}

main().catch(error => {
  console.error('Unhandled Error:', error);
});
