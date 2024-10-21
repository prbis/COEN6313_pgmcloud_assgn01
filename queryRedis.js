// Required Modules
const { createClient } = require('redis');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');
require('dotenv').config(); // Load environment variables from .env

/**
 * Creates a RediSearch index with detailed field specifications.
 * Ensure this is run before performing any queries.
 */
async function createIndex() {
  const client = createClient({
    url: process.env.REDIS_URL,
  });

  client.on('error', (err) => console.error('Redis Client Error', err));

  try {
    await client.connect();
    console.log('Connected to Redis successfully.');

    const indexName = 'idx:prizes';

    // Attempt to drop the index if it exists to avoid duplication errors
    try {
      await client.ft.dropIndex(indexName, { DD: true });
      console.log(`Index "${indexName}" dropped successfully.`);
    } catch (error) {
      if (error.message.includes('Unknown Index name')) {
        console.log(`Index "${indexName}" does not exist. Proceeding to create it.`);
      } else {
        throw error; // Re-throw if it's a different error
      }
    }

    await client.ft.create(
      indexName, // Index name
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
        },
        '$.laureates[*].surname': {
          type: 'TEXT',
          AS: 'surname',
        },
        '$.laureates[*].motivation': {
          type: 'TEXT',
          AS: 'motivation',
        },
      },
      {
        ON: 'JSON', // Specify that we're indexing JSON documents
        PREFIX: ['prize:'], // All keys starting with "prize:" will be indexed
      }
    );

    console.log(`Index "${indexName}" created successfully.`);
  } catch (error) {
    console.error('Error creating index:', error);
  } finally {
    await client.quit();
    console.log('Disconnected from Redis.');
  }
}

/**
 * Retrieves all prizes in the "chemistry" category.
 */
async function performChemistryCategoryQuery() {
  const client = createClient({
    url: process.env.REDIS_URL,
  });

  client.on('error', (err) => console.error('Redis Client Error', err));

  try {
    await client.connect();
    console.log('Connected to Redis successfully.');

    const indexName = 'idx:prizes';
    const query = '@category:{chemistry}';

    console.log(`Executing query on index "${indexName}" with query "${query}"...`);
    const results = await client.ft.search(indexName, query, {
      RETURN: ['$.year', '$.category', '$.laureates'],
      LIMIT: { from: 0, size: 1000 },
    });

    if (results.total > 0) {
      console.log(`\nTotal Results Found: ${results.total}\n`);
      results.documents.forEach((doc, idx) => {
        let laureates;
        try {
          laureates = JSON.parse(doc.value['$.laureates']);
        } catch (parseError) {
          console.error(`Error parsing laureates for result ${idx + 1}:`, parseError);
          laureates = [];
        }
        console.log(`Result ${idx + 1}:`);
        console.log(`Year: ${doc.value['$.year']}`);
        console.log(`Category: ${doc.value['$.category']}`);
        console.log(`Laureates:`);
        laureates.forEach((laureate, laureateIdx) => {
          console.log(`  Laureate ${laureateIdx + 1}:`);
          console.log(`    ID: ${laureate.id}`);
          console.log(`    First Name: ${laureate.firstname}`);
          console.log(`    Surname: ${laureate.surname}`);
          console.log(`    Motivation: ${laureate.motivation}`);
          console.log(`    Share: ${laureate.share}`);
        });
        console.log('---------------------------');
      });
    } else {
      console.log('No results found for category: chemistry');
    }
  } catch (error) {
    console.error('Error performing query:', error);
  } finally {
    await client.quit();
    console.log('Disconnected from Redis.');
  }
}

/**
 * Counts the total number of laureates in a given category and year range.
 * Additionally, it displays detailed information about each laureate.
 */
async function countLaureates(category, startYear, endYear) {
  const client = createClient({
    url: process.env.REDIS_URL,
  });

  client.on('error', (err) => console.error('Redis Client Error', err));

  let totalLaureates = 0;
  const laureatesDetails = []; // To store detailed laureate information

  try {
    await client.connect();
    console.log('Connected to Redis successfully.');

    if (startYear < 2013 || endYear > 2023) {
      throw new Error('Year range must be between 2013 and 2023.');
    }

    const query = `@category:{${category}} @year:[${startYear} ${endYear}]`;
    console.log(`Executing query 1 on index "idx:prizes" with query "${query}"...`);

    const results = await client.ft.search('idx:prizes', query, {
      RETURN: ['$.year', '$.category', '$.laureates'],
      LIMIT: { from: 0, size: 1000 },
    });

    //console.log(`Total Prizes Found: ${results.total}`);

    results.documents.forEach((doc, idx) => {
      let laureates;
      try {
        laureates = JSON.parse(doc.value['$.laureates']);
      } catch (parseError) {
        console.error(`Error parsing laureates for prize ${idx + 1}:`, parseError);
        laureates = [];
      }

      //console.log(`\nPrize ${idx + 1}:`);
      //console.log(`  Year: ${doc.value['$.year']}`);
     // console.log(`  Category: ${doc.value['$.category']}`);
      //console.log(`  Laureates:`);

      if (Array.isArray(laureates)) {
        laureates.forEach((laureate, laureateIdx) => {
          totalLaureates += 1;
         // console.log(`    Laureate ${laureateIdx + 1}:`);
         // console.log(`      ID: ${laureate.id}`);
          //console.log(`      First Name: ${laureate.firstname}`);
         // console.log(`      Surname: ${laureate.surname}`);
         // console.log(`      Motivation: ${laureate.motivation}`);
         // console.log(`      Share: ${laureate.share}`);

          // Store details
          laureatesDetails.push({
            year: doc.value['$.year'],
            category: doc.value['$.category'],
            id: laureate.id,
            firstname: laureate.firstname,
            surname: laureate.surname,
            motivation: laureate.motivation,
            share: laureate.share,
          });
        });
      } else {
        console.warn(`Unexpected laureates format for prize ${idx + 1}:`, laureates);
      }
    });

    console.log(`\nTotal Laureates in category "${category}" from ${startYear} to ${endYear}: ${totalLaureates}`);

    // Optionally, display detailed laureate information
    console.log('\nDetailed Laureate Information:');
    laureatesDetails.forEach((laureate, idx) => {
      console.log(`Laureate ${idx + 1}:`);
      console.log(`  Year: ${laureate.year}`);
      console.log(`  Category: ${laureate.category}`);
      console.log(`  ID: ${laureate.id}`);
      console.log(`  First Name: ${laureate.firstname}`);
      console.log(`  Surname: ${laureate.surname}`);
      console.log(`  Motivation: ${laureate.motivation}`);
      console.log(`  Share: ${laureate.share}`);
      console.log('---------------------------');
    });
  } catch (error) {
    console.error('Error performing query 1:', error);
  } finally {
    await client.quit();
    console.log('Disconnected from Redis.');
  }

  return totalLaureates;
}

/**
 * Counts the total number of laureates with motivations covering a given keyword.
 * Additionally, it displays detailed information about each matching laureate.
 */
async function countLaureatesByMotivation(keyword) {
  const client = createClient({
    url: process.env.REDIS_URL,
  });

  client.on('error', (err) => console.error('Redis Client Error', err));

  let totalMatchingLaureates = 0;
  const matchingLaureatesDetails = []; // To store detailed matching laureate information

  try {
    await client.connect();
    console.log('Connected to Redis successfully.');

    const indexName = 'idx:prizes';
    const query = `@motivation:(${keyword})`;
    console.log(`Executing query 2 on index "${indexName}" with query "${query}"...`);

    const results = await client.ft.search(indexName, query, {
      RETURN: ['$.year', '$.category', '$.laureates'],
      LIMIT: { from: 0, size: 1000 },
    });

    //console.log(`Total Prizes Found with motivations containing "${keyword}": ${results.total}`);

    results.documents.forEach((doc, idx) => {
      let laureates;
      try {
        laureates = JSON.parse(doc.value['$.laureates']);
      } catch (parseError) {
        console.error(`Error parsing laureates for prize ${idx + 1}:`, parseError);
        laureates = [];
      }

      // console.log(`\nPrize ${idx + 1}:`);
      // console.log(`  Year: ${doc.value['$.year']}`);
      // console.log(`  Category: ${doc.value['$.category']}`);
      // console.log(`  Laureates:`);

      if (Array.isArray(laureates)) {
        laureates.forEach((laureate, laureateIdx) => {
          if (laureate.motivation.toLowerCase().includes(keyword.toLowerCase())) {
            totalMatchingLaureates += 1;
            // console.log(`    Laureate ${laureateIdx + 1}:`);
            // console.log(`      ID: ${laureate.id}`);
            // console.log(`      First Name: ${laureate.firstname}`);
            // console.log(`      Surname: ${laureate.surname}`);
            // console.log(`      Motivation: ${laureate.motivation}`);
            // console.log(`      Share: ${laureate.share}`);

            // Store details
            matchingLaureatesDetails.push({
              year: doc.value['$.year'],
              category: doc.value['$.category'],
              id: laureate.id,
              firstname: laureate.firstname,
              surname: laureate.surname,
              motivation: laureate.motivation,
              share: laureate.share,
            });
          }
        });
      } else {
        console.warn(`Unexpected laureates format for prize ${idx + 1}:`, laureates);
      }
    });

    console.log(`\nTotal Laureates with motivations covering the keyword "${keyword}": ${totalMatchingLaureates}`);

    // Optionally, display detailed matching laureate information
    console.log('\nDetailed Matching Laureate Information:');
    matchingLaureatesDetails.forEach((laureate, idx) => {
      console.log(`Laureate ${idx + 1}:`);
      console.log(`  Year: ${laureate.year}`);
      console.log(`  Category: ${laureate.category}`);
      console.log(`  ID: ${laureate.id}`);
      console.log(`  First Name: ${laureate.firstname}`);
      console.log(`  Surname: ${laureate.surname}`);
      console.log(`  Motivation: ${laureate.motivation}`);
      console.log(`  Share: ${laureate.share}`);
      console.log('---------------------------');
    });
  } catch (error) {
    console.error('Error performing query 2:', error);
  } finally {
    await client.quit();
    console.log('Disconnected from Redis.');
  }

  return totalMatchingLaureates;
}

/**
 * Retrieves the year, category, and motivation of a laureate based on their first and last names.
 */
async function getLaureateDetails(firstname, surname) {
  const client = createClient({
    url: process.env.REDIS_URL,
  });

  client.on('error', (err) => console.error('Redis Client Error', err));

  try {
    await client.connect();
    console.log('Connected to Redis successfully.');

    // Construct the query to match firstname and surname using exact match
    const query = `@firstname:=="${firstname}" @surname:=="${surname}"`;

    console.log(`Executing query 3 on index "idx:prizes" with query "${query}"...`);

    // Perform search and return the full JSON document along with the key
    const results = await client.ft.search('idx:prizes', query, {
      RETURN: ['$', '__key'],
      LIMIT: { from: 0, size: 1000 },
    });

    if (results.total === 0) {
      console.log(`\nNo laureate found with name "${firstname} ${surname}".`);
      return;
    }

    const laureatesInfo = [];

    for (const doc of results.documents) {
      //console.log('Document Value:', doc.value);

      // Attempt to parse the JSON document
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
          const rawData = await client.json.get(doc.value['__key']);
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
          // Combine prize-level fields (year, category) with the laureate's motivation
          const laureateDetails = {
            year: year,
            category: category,
            motivation: matchingLaureate.motivation,
          };

          laureatesInfo.push(laureateDetails);
        }
      }
    }

    if (laureatesInfo.length > 0) {
      console.log('\nLaureate Details:');
      console.log(JSON.stringify(laureatesInfo, null, 2));
    } else {
      console.log(`\nNo matching laureate details found for "${firstname} ${surname}".`);
    }
  } catch (error) {
    console.error('Error performing query 3:', error);
  } finally {
    await client.quit();
    console.log('Disconnected from Redis.');
  }
}

// Command-line argument handling
const argv = yargs(hideBin(process.argv))
  .command('createIndex', 'Create the RediSearch index for prizes', {})
  .command('query ', 'Retrieve all prizes in the chemistry category', {})
  .command('query1', 'Count total laureates in a category and year range', {
    category: {
      description: 'The category of the prize',
      alias: 'c',
      type: 'string',
      demandOption: true,
    },
    startYear: {
      description: 'The starting year',
      alias: 's',
      type: 'number',
      demandOption: true,
    },
    endYear: {
      description: 'The ending year',
      alias: 'e',
      type: 'number',
      demandOption: true,
    },
  })
  .command('query2', 'Count total laureates by motivation keyword', {
    keyword: {
      description: 'Keyword for motivation search',
      alias: 'k',
      type: 'string',
      demandOption: true,
    },
  })
  .command('query3', 'Retrieve details of a laureate by their name', {
    firstname: {
      description: 'First name of the laureate',
      alias: 'f',
      type: 'string',
      demandOption: true,
    },
    surname: {
      description: 'Surname of the laureate',
      alias: 's',
      type: 'string',
      demandOption: true,
    },
  })
  .help()
  .argv;

// Command execution based on user input
async function main() {
  const { _: commands, category, startYear, endYear, keyword, firstname, surname } = argv;

  if (commands.includes('createIndex')) {
    await createIndex();
  } else if (commands.includes('query ')) {
    await performChemistryCategoryQuery();
  } else if (commands.includes('query1')) {
    if (category && startYear && endYear) {
      await countLaureates(category, startYear, endYear);
    } else {
      console.log('Please provide category, startYear, and endYear for query 1.');
    }
  } else if (commands.includes('query2')) {
    if (keyword) {
      await countLaureatesByMotivation(keyword);
    } else {
      console.log('Please provide a keyword for query 2.');
    }
  } else if (commands.includes('query3')) {
    if (firstname && surname) {
      await getLaureateDetails(firstname, surname);
    } else {
      console.log('Please provide both firstname and surname for query 3.');
    }
  } else {
    console.log('Unknown command. Please use --help to see available commands.');
  }
}

// Execute the main function
main().catch((error) => console.error('Unhandled Error:', error));
