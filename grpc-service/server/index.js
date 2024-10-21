// server/index.js

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { createClient } = require('redis');
require('dotenv').config();

// Path to the .proto file
//const PROTO_PATH = __dirname + '/../protos/prize_service.proto';

// Update this line in your index.js
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

// Initialize Redis client
const redisClient = createClient({
  url: process.env.REDIS_URL,
});

redisClient.on('error', (err) => {
  console.error('Redis Client Error', err);
});

// Connect to Redis
redisClient.connect()
  .then(() => console.log('Connected to Redis successfully.'))
  .catch((err) => console.error('Redis Connection Error:', err));

// Implementations of RPC methods

/**
 * GetPrizesByCategory: Retrieves all prizes in the "chemistry" category.
 */
async function GetPrizesByCategory(call, callback) {
  try {
    const indexName = 'idx:prizes';
    const query = '@category:{chemistry}';
    
    const results = await redisClient.ft.search(indexName, query, {
      RETURN: ['$.year', '$.category', '$.laureates'],
      LIMIT: { from: 0, size: 1000 },
    });

    const prizes = results.documents.map(doc => {
      let laureates = [];
      try {
        laureates = JSON.parse(doc.value['$.laureates']);
      } catch (parseError) {
        console.error(`Error parsing laureates for prize:`, parseError);
      }

      return {
        year: doc.value['$.year'],
        category: doc.value['$.category'],
        laureates: laureates.map(laureate => ({
          id: laureate.id,
          firstname: laureate.firstname,
          surname: laureate.surname,
          motivation: laureate.motivation,
          share: laureate.share
        }))
      };
    });

    callback(null, { prizes });
  } catch (error) {
    console.error('Error in GetPrizesByCategory:', error);
    callback({
      code: grpc.status.INTERNAL,
      message: 'Internal server error'
    });
  }
}

/**
 * CountLaureatesByCategoryAndYearRange: Counts total laureates in a category and year range.
 */
async function CountLaureatesByCategoryAndYearRange(call, callback) {
  try {
    const { category, startYear, endYear } = call.request;

    if (startYear < 2013 || endYear > 2023) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: 'Year range must be between 2013 and 2023.'
      });
    }

    const indexName = 'idx:prizes';
    const query = `@category:{${category}} @year:[${startYear} ${endYear}]`;

    const results = await redisClient.ft.search(indexName, query, {
      RETURN: ['$.year', '$.category', '$.laureates'],
      LIMIT: { from: 0, size: 1000 },
    });

    let totalLaureates = 0;
    const laureates = [];

    results.documents.forEach(doc => {
      let laureatesList = [];
      try {
        laureatesList = JSON.parse(doc.value['$.laureates']);
      } catch (parseError) {
        console.error(`Error parsing laureates for prize:`, parseError);
      }

      laureatesList.forEach(laureate => {
        totalLaureates += 1;
        laureates.push({
          year: doc.value['$.year'],
          category: doc.value['$.category'],
          id: laureate.id,
          firstname: laureate.firstname,
          surname: laureate.surname,
          motivation: laureate.motivation,
          share: laureate.share
        });
      });
    });

    callback(null, {
      totalLaureates,
      laureates
    });
  } catch (error) {
    console.error('Error in CountLaureatesByCategoryAndYearRange:', error);
    callback({
      code: grpc.status.INTERNAL,
      message: 'Internal server error'
    });
  }
}

/**
 * CountLaureatesByMotivationKeyword: Counts laureates with motivations containing a keyword.
 */
async function CountLaureatesByMotivationKeyword(call, callback) {
  try {
    const { keyword } = call.request;

    if (!keyword || keyword.trim() === '') {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: 'Keyword cannot be empty.'
      });
    }

    const indexName = 'idx:prizes';
    const query = `@motivation:(${keyword})`;

    const results = await redisClient.ft.search(indexName, query, {
      RETURN: ['$.year', '$.category', '$.laureates'],
      LIMIT: { from: 0, size: 1000 },
    });

    let totalMatchingLaureates = 0;
    const laureates = [];

    results.documents.forEach(doc => {
      let laureatesList = [];
      try {
        laureatesList = JSON.parse(doc.value['$.laureates']);
      } catch (parseError) {
        console.error(`Error parsing laureates for prize:`, parseError);
      }

      laureatesList.forEach(laureate => {
        if (laureate.motivation.toLowerCase().includes(keyword.toLowerCase())) {
          totalMatchingLaureates += 1;
          laureates.push({
            year: doc.value['$.year'],
            category: doc.value['$.category'],
            id: laureate.id,
            firstname: laureate.firstname,
            surname: laureate.surname,
            motivation: laureate.motivation,
            share: laureate.share
          });
        }
      });
    });

    callback(null, {
      totalLaureates: totalMatchingLaureates,
      laureates
    });
  } catch (error) {
    console.error('Error in CountLaureatesByMotivationKeyword:', error);
    callback({
      code: grpc.status.INTERNAL,
      message: 'Internal server error'
    });
  }
}

/**
 * GetLaureateDetailsByName: Retrieves details of a laureate by their first and last names.
 */
async function GetLaureateDetailsByName(call, callback) {
  try {
    const { firstname, surname } = call.request;

    if (!firstname || !surname) {
      return callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: 'First name and surname are required.'
      });
    }

    const indexName = 'idx:prizes';
    const query = `@firstname:=="${firstname}" @surname:=="${surname}"`;

    const results = await redisClient.ft.search(indexName, query, {
      RETURN: ['$', '__key'],
      LIMIT: { from: 0, size: 1000 },
    });

    if (results.total === 0) {
      return callback(null, { laureates: [] });
    }

    const laureatesInfo = [];

    for (const doc of results.documents) {
      let data;
      if (doc.value['$']) {
        try {
          data = JSON.parse(doc.value['$']);
        } catch (parseError) {
          console.error('Error parsing JSON from doc.value["$"]:', parseError);
          continue; // Skip this document
        }
      } else {
        // If '$' field is missing, fetch the JSON directly from Redis
        try {
          const rawData = await redisClient.json.get(doc.value['__key']);
          data = rawData;
        } catch (fetchError) {
          console.error(`Error fetching JSON data for key "${doc.value['__key']}":`, fetchError);
          continue; // Skip this document
        }
      }

      const { year, category, laureates } = data;

      if (Array.isArray(laureates)) {
        const matchingLaureate = laureates.find(
          (laureate) =>
            laureate.firstname.toLowerCase() === firstname.toLowerCase() &&
            laureate.surname.toLowerCase() === surname.toLowerCase()
        );

        if (matchingLaureate) {
          laureatesInfo.push({
            year: year,
            category: category,
            motivation: matchingLaureate.motivation
          });
        }
      }
    }

    callback(null, { laureates: laureatesInfo });
  } catch (error) {
    console.error('Error in GetLaureateDetailsByName:', error);
    callback({
      code: grpc.status.INTERNAL,
      message: 'Internal server error'
    });
  }
}

// Start the gRPC server
function main() {
  const server = new grpc.Server();
  server.addService(prizeProto.PrizeService.service, {
    GetPrizesByCategory,
    CountLaureatesByCategoryAndYearRange,
    CountLaureatesByMotivationKeyword,
    GetLaureateDetailsByName
  });
  //const bindAddress = '0.0.0.0:50051';
  const bindAddress = `0.0.0.0:${process.env.PORT || 50051}`;
  server.bindAsync(bindAddress, grpc.ServerCredentials.createInsecure(), () => {
    server.start();
    console.log(`gRPC server running at ${bindAddress}`);
  });
}

main();
