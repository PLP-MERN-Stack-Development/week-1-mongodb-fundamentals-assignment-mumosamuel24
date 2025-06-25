// queries.js

const { MongoClient } = require('mongodb');

const uri = 'mongodb://localhost:27017';
const dbName = 'plp_bookstore';
const collectionName = 'books';

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('Connected to MongoDB');

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // 1. Find all books in a specific genre
    const genre = 'Fiction';
    const booksByGenre = await collection.find({ genre }).toArray();
    console.log(`\nBooks in genre '${genre}':`, booksByGenre);

    // 2. Find books published after a certain year
    const year = 1950;
    const booksAfterYear = await collection.find({ published_year: { $gt: year } }).toArray();
    console.log(`\nBooks published after ${year}:`, booksAfterYear);

    // 3. Find books by a specific author
    const author = 'George Orwell';
    const booksByAuthor = await collection.find({ author }).toArray();
    console.log(`\nBooks by '${author}':`, booksByAuthor);

    // 4. Update the price of a specific book by title
    const titleToUpdate = '1984';
    const newPrice = 15.99;
    const updateResult = await collection.updateOne(
      { title: titleToUpdate },
      { $set: { price: newPrice } }
    );
    console.log(`\nUpdated price of '${titleToUpdate}':`, updateResult.modifiedCount === 1);

    // 5. Delete a book by its title
    const titleToDelete = 'Animal Farm';
    const deleteResult = await collection.deleteOne({ title: titleToDelete });
    console.log(`\nDeleted '${titleToDelete}':`, deleteResult.deletedCount === 1);

    // Task 3: Advanced Queries

    // Find books both in stock and published after 2010
    const advancedBooks = await collection.find({
      in_stock: true,
      published_year: { $gt: 2010 }
    }).toArray();
    console.log('\nBooks in stock and published after 2010:', advancedBooks);

    // Projection: return only title, author, and price fields
    const projectionBooks = await collection.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).toArray();
    console.log('\nProjection (title, author, price) of all books:', projectionBooks);

    // Sorting by price ascending
    const sortAsc = await collection.find().sort({ price: 1 }).toArray();
    console.log('\nBooks sorted by price ascending:', sortAsc);

    // Sorting by price descending
    const sortDesc = await collection.find().sort({ price: -1 }).toArray();
    console.log('\nBooks sorted by price descending:', sortDesc);

    // Pagination: 5 books per page, page 2 (skip 5)
    const pageSize = 5;
    const pageNumber = 2;
    const paginatedBooks = await collection.find()
      .skip(pageSize * (pageNumber - 1))
      .limit(pageSize)
      .toArray();
    console.log(`\nPage ${pageNumber} with ${pageSize} books per page:`, paginatedBooks);

    // Task 4: Aggregation Pipeline

    // 1. Average price of books by genre
    const avgPriceByGenre = await collection.aggregate([
      { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } },
      { $sort: { avgPrice: -1 } }
    ]).toArray();
    console.log('\nAverage price by genre:', avgPriceByGenre);

    // 2. Author with the most books
    const authorMostBooks = await collection.aggregate([
      { $group: { _id: "$author", bookCount: { $sum: 1 } } },
      { $sort: { bookCount: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log('\nAuthor with most books:', authorMostBooks);

    // 3. Group books by publication decade and count them
    const booksByDecade = await collection.aggregate([
      {
        $group: {
          _id: {
            $concat: [
              { $toString: { $subtract: ["$published_year", { $mod: ["$published_year", 10] }] } },
              "s"
            ]
          },
          count: { $sum: 1 }
        }
      },
      { $sort: { _id: 1 } }
    ]).toArray();
    console.log('\nBooks grouped by decade:', booksByDecade);

    // Task 5: Indexing

    // Create an index on title field
    await collection.createIndex({ title: 1 });
    console.log('\nCreated index on title');

    // Create a compound index on author and published_year
    await collection.createIndex({ author: 1, published_year: -1 });
    console.log('Created compound index on author and published_year');

    // Explain query performance with and without index
    const explainBefore = await collection.find({ title: "1984" }).explain("executionStats");
    console.log('\nExplain for query on title "1984":', JSON.stringify(explainBefore.executionStats, null, 2));

    const explainCompound = await collection.find({ author: "George Orwell", published_year: 1949 }).explain("executionStats");
    console.log('\nExplain for query on author "George Orwell" and published_year 1949:', JSON.stringify(explainCompound.executionStats, null, 2));

  } catch (err) {
    console.error('Error:', err);
  } finally {
    await client.close();
    console.log('Connection closed');
  }
}

runQueries();
