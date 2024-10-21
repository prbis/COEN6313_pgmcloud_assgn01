// Required Modules
const { createClient } = require('redis');
const fetch = require('node-fetch'); // Ensure you've installed node-fetch: npm install node-fetch
require('dotenv').config(); // Load environment variables from .env

/**
 * Fetches Nobel Prize data from the official API.
 * @returns {Promise<Object>} The fetched data in JSON format.
 */
async function fetchNobelData() {
  const response = await fetch('http://api.nobelprize.org/v1/prize.json');
  if (!response.ok) {
    throw new Error(`Failed to fetch data: ${response.status} ${response.statusText}`);
  }
  const data = await response.json();
  return data;
}

/**
 * Filters the fetched data to include only prizes within the specified year range.
 * @param {Object} data - The complete data fetched from the API.
 * @param {number} startYear - The starting year for filtering.
 * @param {number} endYear - The ending year for filtering.
 * @returns {Array} An array of filtered prize objects.
 */
function filterDataByYear(data, startYear, endYear) {
  const filteredPrizes = data.prizes.filter((prize) => {
    const year = parseInt(prize.year, 10);
    return year >= startYear && year <= endYear;
  });
  return filteredPrizes;
}

/**
 * Creates a vector-like field from laureates' full names.
 * @param {Array} laureates - An array of laureate objects.
 * @returns {string} A concatenated string of full names separated by ' | '.
 */
function createVectorField(laureates) {
  return laureates
    .map((laureate) => {
      const fullName = `${laureate.firstname || ''} ${laureate.surname || ''}`.trim();
      return fullName;
    })
    .join(' | '); // Separate laureate names with '|' for better indexing
}

/**
 * Uploads the filtered Nobel Prize data to Redis.
 * @param {Array} filteredData - An array of filtered prize objects.
 * @param {RedisClient} client - The Redis client instance.
 */
async function uploadDataToRedis(filteredData, client) {
  // Initialize counters for each year and category to ensure unique keys
  const keyCounters = {};

  for (const prize of filteredData) {
    const { year, category } = prize;
    const keyBase = `prize:${year}:${category}`;

    // Initialize counter if not already set
    if (!keyCounters[keyBase]) {
      keyCounters[keyBase] = 1;
    }

    // Use the counter for the current key
    const key = `${keyBase}:${keyCounters[keyBase]}`;
    keyCounters[keyBase] += 1; // Increment the counter for the next key

    const vectorField = createVectorField(prize.laureates || []);

    // Convert 'year' to a number to match the NUMERIC index type
    const dataToSave = {
      ...prize,
      vectorField,
      year: parseInt(prize.year, 10), // Ensure 'year' is a number
    };

    // Debugging: Log the data being saved
    // console.log(`Data being saved for key ${key}:`, JSON.stringify(dataToSave, null, 2));

    await client.json.set(key, '.', dataToSave); // Save the data as a JSON document
    // console.log(`Uploaded prize: ${key}`);
  }
}

/**
 * Creates a RediSearch index with detailed field specifications.
 * @param {RedisClient} client - The Redis client instance.
 */
async function createRediSearchIndex(client) {
  // Attempt to drop the existing index if it exists
  try {
    await client.ft.dropIndex('idx:prizes', { DD: false });
    console.log('Deleted existing index: idx:prizes');
  } catch (error) {
    if (error.message.includes('Unknown Index name')) {
      console.log('No existing index to delete.');
    } else {
      throw error; // Re-throw unexpected errors
    }
  }

  // Create a RediSearch index with detailed field specifications
  try {
    await client.ft.create(
      'idx:prizes', // Index name
      {
        '$.year': {
          type: 'NUMERIC',
          AS: 'year',
          SORTABLE: true, // Enable sorting on the 'year' field
        },
        '$.category': {
          type: 'TAG',
          AS: 'category',
          SEPARATOR: ',', // Define a separator for TAG fields if needed
        },
        '$.laureates[*].firstname': {
          type: 'TEXT',
          AS: 'firstname',
          WEIGHT: 1, // Optional: Define weight for full-text search relevance
        },
        '$.laureates[*].surname': {
          type: 'TEXT',
          AS: 'surname',
          WEIGHT: 1,
        },
        '$.laureates[*].motivation': {
          type: 'TEXT',
          AS: 'motivation',
          WEIGHT: 1,
        },
        '$.vectorField': {
          type: 'TEXT',
          AS: 'vectorField',
          WEIGHT: 1,
        },
      },
      {
        ON: 'JSON', // Specify that we're indexing JSON documents
        PREFIX: ['prize:'], // All keys starting with "prize:" will be indexed
      }
    );
    console.log('Index with detailed fields created successfully.');
  } catch (error) {
    if (error.message.includes('Index already exists')) {
      console.log('Index already exists, skipping creation.');
    } else {
      throw error; // Re-throw unexpected errors
    }
  }
}

/**
 * Verifies the uploaded data by retrieving a specific key from Redis.
 * @param {RedisClient} client - The Redis client instance.
 */
async function verifyDataAfterUpload(client) {
  const key = 'prize:2013:chemistry:1'; // Adjust this key as needed
  const data = await client.json.get(key);
  if (data) {
    console.log(`Uploaded Data for key ${key}:`, JSON.stringify(data, null, 2));
  } else {
    console.log(`No data found for key: ${key}`);
  }
}

/**
 * Performs a RediSearch query to verify that the index works as expected.
 * @param {RedisClient} client - The Redis client instance.
 */
async function checkRedisQuery(client) {
  const results = await client.ft.search('idx:prizes', '@category:{chemistry}');
  if (results.total > 0) {
    console.log('Search Results:', JSON.stringify(results, null, 2));
  } else {
    console.log('No results found for category: chemistry');
  }
}

/**
 * The main function orchestrates fetching, filtering, uploading, verifying, and querying data.
 */
async function main() {
  const startYear = 2013;
  const endYear = 2023;

  console.log('Fetching Nobel Prize data...');
  const data = await fetchNobelData();

  console.log(`Filtering data from ${startYear} to ${endYear}...`);
  const filteredData = filterDataByYear(data, startYear, endYear);

  console.log('Connecting to Redis...');
  const client = createClient({
    url: process.env.REDIS_URL, // Ensure your .env file contains REDIS_URL
  });

  client.on('error', (err) => console.error('Redis Client Error', err));

  try {
    await client.connect();
    console.log('Connected to Redis successfully.');

    // Upload data to Redis
    console.log('Uploading filtered data to Redis...');
    await uploadDataToRedis(filteredData, client);

    // Create RediSearch index
    console.log('Creating RediSearch index...');
    await createRediSearchIndex(client);

    // Verify data
    console.log('Verifying the uploaded data...');
    await verifyDataAfterUpload(client);

    // Perform a sample query
    console.log('Performing a simple query to verify the data...');
    await checkRedisQuery(client);
  } catch (error) {
    console.error('Error during execution:', error);
  } finally {
    await client.quit();
    console.log('Disconnected from Redis.');
  }
}

// Execute the main function and handle any uncaught errors
main().catch((error) => console.error('Error in main execution:', error));
