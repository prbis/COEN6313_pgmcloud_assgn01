// client/index.js

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
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

// Helper function to handle gRPC responses
function handleResponse(error, response) {
  if (error) {
    console.error('Error:', error.message);
  } else {
    console.log('Response:', JSON.stringify(response, null, 2));
  }
}

// Command-line argument handling using yargs
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const argv = yargs(hideBin(process.argv))
  .command(
    'query1',
    'Retrieve all prizes in the "chemistry" category.',
    () => {},
    (args) => {
      client.GetPrizesByCategory({}, handleResponse);
    }
  )
  .command(
    'query2',
    'Count the total number of laureates in a given category and year range.',
    (yargs) => {
      return yargs
        .option('category', {
          alias: 'c',
          type: 'string',
          description: 'Category to search for (e.g., "chemistry").',
          demandOption: true,
        })
        .option('startYear', {
          alias: 's',
          type: 'number',
          description: 'Start year of the range (inclusive).',
          demandOption: true,
        })
        .option('endYear', {
          alias: 'e',
          type: 'number',
          description: 'End year of the range (inclusive).',
          demandOption: true,
        });
    },
    (args) => {
      client.CountLaureatesByCategoryAndYearRange({
        category: args.category,
        startYear: args.startYear,
        endYear: args.endYear
      }, handleResponse);
    }
  )
  .command(
    'query3',
    'Count the total number of laureates with motivations covering a given keyword.',
    (yargs) => {
      return yargs.option('keyword', {
        alias: 'k',
        type: 'string',
        description: 'Keyword to search for in motivations.',
        demandOption: true,
      });
    },
    (args) => {
      client.CountLaureatesByMotivationKeyword({
        keyword: args.keyword
      }, handleResponse);
    }
  )
  .command(
    'query4',
    'Retrieve details of a laureate by their name.',
    (yargs) => {
      return yargs
        .option('firstname', {
          alias: 'f',
          type: 'string',
          description: 'First name of the laureate.',
          demandOption: true,
        })
        .option('surname', {
          alias: 's',
          type: 'string',
          description: 'Surname of the laureate.',
          demandOption: true,
        });
    },
    (args) => {
      client.GetLaureateDetailsByName({
        firstname: args.firstname,
        surname: args.surname
      }, handleResponse);
    }
  )
  .demandCommand(1, 'You need to specify at least one command.')
  .help()
  .argv;
