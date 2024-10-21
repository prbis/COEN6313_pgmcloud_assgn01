// client/performanceTest.js

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');
const fs = require('fs');
const _ = require('lodash');
require('dotenv').config();

// Path to the .proto file
const PROTO_PATH = __dirname + '/../protos/prize_service.proto';

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

// Helper function to handle gRPC responses with Promise
function callRPC(method, request) {
  return new Promise((resolve, reject) => {
    client[method](request, (error, response) => {
      if (error) {
        reject(error);
      } else {
        resolve(response);
      }
    });
  });
}

// Function to measure E2E delay for a single RPC call
async function measureDelay(method, request) {
  const startTime = process.hrtime(); // High-resolution real time
  try {
    await callRPC(method, request);
  } catch (error) {
    console.error(`Error in method ${method}:`, error.message);
    // You may choose to handle errors differently
  }
  const elapsed = process.hrtime(startTime);
  const elapsedTimeInMs = (elapsed[0] * 1e9 + elapsed[1]) / 1e6; // Convert to milliseconds
  return elapsedTimeInMs;
}

// Main function to perform performance tests
async function main() {
  const totalRuns = 100;

  // Arrays to store delay times for each query
  const delays = {
    query1: [],
    query2: [],
    query3: [],
    query4: []
  };

  console.log(`Starting performance tests: ${totalRuns} runs for each of the four queries.\n`);

  for (let i = 1; i <= totalRuns; i++) {
    console.log(`Run ${i} of ${totalRuns}`);

    // --- Query 1: GetPrizesByCategory ---
    const delay1 = await measureDelay('GetPrizesByCategory', {});
    delays.query1.push(delay1);
    console.log(`  Query1 (GetPrizesByCategory) Delay: ${delay1.toFixed(2)} ms`);

    // --- Query 2: CountLaureatesByCategoryAndYearRange ---
    // Example parameters; adjust as needed
    const countRequest = {
      category: 'physics',
      startYear: 2013,
      endYear: 2023
    };
    const delay2 = await measureDelay('CountLaureatesByCategoryAndYearRange', countRequest);
    delays.query2.push(delay2);
    console.log(`  Query2 (CountLaureatesByCategoryAndYearRange) Delay: ${delay2.toFixed(2)} ms`);

    // --- Query 3: CountLaureatesByMotivationKeyword ---
    const motivationRequest = {
      keyword: 'development'
    };
    const delay3 = await measureDelay('CountLaureatesByMotivationKeyword', motivationRequest);
    delays.query3.push(delay3);
    console.log(`  Query3 (CountLaureatesByMotivationKeyword) Delay: ${delay3.toFixed(2)} ms`);

    // --- Query 4: GetLaureateDetailsByName ---
    const nameRequest = {
      firstname: 'Arthur',
      surname: 'Ashkin'
    };
    const delay4 = await measureDelay('GetLaureateDetailsByName', nameRequest);
    delays.query4.push(delay4);
    console.log(`  Query4 (GetLaureateDetailsByName) Delay: ${delay4.toFixed(2)} ms\n`);

    // Optional: Introduce a short delay between runs to avoid overwhelming the server
    await new Promise(resolve => setTimeout(resolve, 100)); // 100 ms delay
  }

  // Save the delays to a JSON file
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const outputFileName = `delays_${timestamp}.json`;
  fs.writeFileSync(__dirname + `/${outputFileName}`, JSON.stringify(delays, null, 2));
  console.log(`Performance testing completed. Delays saved to ${outputFileName}`);
}

main().catch((error) => {
  console.error('Unhandled Error:', error);
});
