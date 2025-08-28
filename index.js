require('dotenv').config();
const path = require('path');
const SqliteDbAdapter = require('./db/sqlLiteDbAdaptor');

// Create database path
const dbPath = path.join(__dirname, 'sqlite', 'data.sqlite');
const db = new SqliteDbAdapter(dbPath);

async function initializeDatabase() {
    try {
        await db.connect();
        
        // Create a test table
        await db.exec(`
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        `);
        
        console.log('Database initialized successfully');
    } catch (error) {
        console.error('Error initializing database:', error);
        throw error;
    }
}

async function testDatabaseOperations() {
    try {
        // Insert test data
        const insertResult = await db.run(
            'INSERT INTO users (name, email) VALUES (?, ?)',
            ['John Doe', 'john@example.com']
        );
        console.log('Insert result:', insertResult);

        // Select single user
        const user = await db.get('SELECT * FROM users WHERE id = ?', [insertResult.id]);
        console.log('Selected user:', user);

        // Insert multiple users
        await db.run(
            'INSERT INTO users (name, email) VALUES (?, ?)',
            ['Jane Smith', 'jane@example.com']
        );
        await db.run(
            'INSERT INTO users (name, email) VALUES (?, ?)',
            ['Bob Wilson', 'bob@example.com']
        );

        // Select all users
        const allUsers = await db.all('SELECT * FROM users');
        console.log('All users:', allUsers);

        // Select users with specific condition
        const filteredUsers = await db.all(
            'SELECT * FROM users WHERE name LIKE ?',
            ['%John%']
        );
        console.log('Filtered users:', filteredUsers);

    } catch (error) {
        console.error('Error during database operations:', error);
        throw error;
    }
}

async function main() {
    try {
        await initializeDatabase();
        await testDatabaseOperations();
        console.log('All database operations completed successfully');
    } catch (error) {
        console.error('Main error:', error);
    } finally {
        await db.close();
        console.log('Database connection closed');
    }
}

main();
