// uploadnobeldata.js

const { createClient } = require('redis');
const Buffer = require('buffer').Buffer;
require('dotenv').config(); // For environment variables

/**
 * Function to fetch Nobel Prize data from the API.
 * @returns {Promise<Object>} - The fetched Nobel Prize data.
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
 * Function to filter prizes within a specified year range.
 * @param {Object} data - The complete Nobel Prize data.
 * @param {number} startYear - The start year for filtering.
 * @param {number} endYear - The end year for filtering.
 * @returns {Array} - An array of filtered prizes.
 */
async function filterDataByYear(data, startYear, endYear) {
  const filteredPrizes = data.prizes.filter((prize) => {
    const year = parseInt(prize.year, 10);
    return !isNaN(year) && year >= startYear && year <= endYear;
  });
  return filteredPrizes;
}

/**
 * Character Code Embedding Function
 * Converts a full name string into a numerical vector based on character ASCII codes.
 * @param {string} fullName - The full name of the laureate.
 * @returns {Array<number>} - A numerical vector of length 128.
 */
function someVectorEmbeddingFunction(fullName) {
  const vector = new Array(128).fill(0);
  for (let i = 0; i < Math.min(fullName.length, 128); i++) {
    vector[i] = fullName.charCodeAt(i) / 255; // Normalize ASCII code
  }
  return vector;
}

/**
 * Function to generate a vector for a laureate.
 * @param {Object} laureate - The laureate object.
 * @returns {Array<number>} - A numerical vector of length 128.
 */
function generateLaureateVector(laureate) {
  const fullName = `${laureate.firstname || ''} ${laureate.surname || ''}`.trim();
  return someVectorEmbeddingFunction(fullName);
}

/**
 * Function to encode a vector array into a binary string.
 * Converts an array of numbers into a Buffer and then to a binary string using 'latin1' encoding.
 * @param {Array<number>} vector - The numerical vector.
 * @returns {string} - The binary string representation of the vector.
 */
function encodeVector(vector) {
  const buffer = Buffer.alloc(4 * vector.length); // FLOAT32, 4 bytes each
  vector.forEach((value, index) => {
    buffer.writeFloatLE(value, index * 4);
  });
  return buffer.toString('latin1'); // Binary-safe string
}

/**
 * Function to validate laureate data.
 * @param {Object} laureate - The laureate object.
 * @throws Will throw an error if validation fails.
 */
function validateLaureate(laureate) {
  if (!laureate.firstname && !laureate.surname) {
    throw new Error('Laureate must have at least a firstname or surname.');
  }
  if (!laureate.motivation) {
    laureate.motivation = '';
  }
}

/**
 * Function to safely set JSON in Redis with retries.
 * @param {Object} client - The Redis client.
 * @param {string} key - The Redis key.
 * @param {string} path - The JSON path.
 * @param {Object} data - The data to set.
 * @param {number} retries - Current retry count.
 */
async function safeJsonSet(client, key, path, data, retries = 0) {
  const MAX_RETRIES = 3;
  try {
    await client.json.set(key, path, data);
  } catch (error) {
    if (retries < MAX_RETRIES) {
      console.warn(`Retrying json.set for key: ${key} (Attempt ${retries + 1})`);
      await safeJsonSet(client, key, path, data, retries + 1);
    } else {
      console.error(`Failed to set key ${key} after ${MAX_RETRIES} attempts:`, error);
    }
  }
}

/**
 * Function to upload filtered Nobel Prize data to Redis.
 * Creates one document per laureate.
 * @param {Array} filteredData - An array of filtered prize objects.
 */
async function uploadDataToRedis(filteredData) {
  const client = createClient({
    // Use environment variable for Redis URL
    url: process.env.REDIS_URL,
  });

  client.on('error', (err) => console.error('Redis Client Error', err));

  try {
    await client.connect();

    let laureateIndex = 1;

    // Optional: Drop existing index to avoid schema conflicts
    try {
      await client.ft.dropIndex('idx:laureates');
      console.log('Existing index "idx:laureates" dropped.');
    } catch (dropError) {
      if (dropError.message.includes('Unknown Index name')) {
        console.log('Index "idx:laureates" does not exist, no need to drop.');
      } else {
        throw dropError;
      }
    }

    // Iterate over the filtered data and upload each laureate as a separate document
    for (const prize of filteredData) {
      const { year, category, laureates } = prize;
      if (laureates && laureates.length > 0) {
        for (const laureate of laureates) {
          try {
            validateLaureate(laureate);
          } catch (validationError) {
            console.error(`Validation error for laureate: ${JSON.stringify(laureate)} - ${validationError.message}`);
            continue; // Skip invalid laureate
          }

          const firstName = laureate.firstname || '';
          const lastName = laureate.surname || '';
          const fullName = `${firstName} ${lastName}`.trim().replace(/\s+/g, '_'); // Replace spaces with underscores for key
          const key = `laureate:${year}:${category}:${laureateIndex}`;
          const vector = generateLaureateVector(laureate);
          const encodedVector = encodeVector(vector);

          // Parse year as integer and validate
          const parsedYear = parseInt(year, 10);
          if (isNaN(parsedYear)) {
            console.error(`Invalid year (${year}) for prize: ${JSON.stringify(prize)}. Skipping laureate.`);
            continue; // Skip laureate with invalid year
          }

          console.log(`Uploading laureate data for key: ${key}`);
          console.log({
            year: parsedYear, // Ensure year is a number
            category,
            motivation: laureate.motivation || '',
            vector: encodedVector
          });

          // Set the JSON object in Redis with 'year' as a number
          await safeJsonSet(client, key, '.', {
            year: parsedYear, // Convert year to number
            category,
            motivation: laureate.motivation || '',
            vector: encodedVector
          });

          console.log(`Uploaded laureate: ${key}`);
          laureateIndex++;
        }
      }
    }

    // Create an index on the JSON documents with vector field
    try {
      await client.ft.create(
        'idx:laureates',
        [
          '$.year', 'NUMERIC', 'SORTABLE', 'AS', 'year',
          '$.category', 'TAG', 'SORTABLE', 'AS', 'category',
          '$.motivation', 'TEXT', 'AS', 'motivation',
          '$.vector', 'VECTOR', 'FLAT', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'COSINE', 'AS', 'vector'
        ],
        {
          ON: 'JSON',
          PREFIX: 'laureate:',
        }
      );
      console.log('Index with vector field created successfully.');
    } catch (error) {
      if (error.message.includes('Index already exists')) {
        console.log('Index already exists, skipping creation.');
      } else {
        throw error;
      }
    }

  } catch (error) {
    console.error('Error uploading data to Redis:', error);
  } finally {
    await client.quit();
  }
}

/**
 * Main function to orchestrate the workflow.
 */
async function main() {
  const startYear = 2013;
  const endYear = 2023;

  console.log('Fetching Nobel Prize data...');
  const data = await fetchNobelData();

  console.log(`Filtering data from ${startYear} to ${endYear}...`);
  const filteredData = await filterDataByYear(data, startYear, endYear);

  console.log('Uploading filtered data to Redis...');
  await uploadDataToRedis(filteredData);

  console.log('Data upload complete.');
}

// Execute the main function
main().catch((error) => console.error('Error in main execution:', error));
