const express = require('express');
const mysql = require('mysql');
const path = require('path');
const bodyParser = require('body-parser');
const app = express();

app.use(express.static(path.join(__dirname, 'public')));

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.post('/execute-query', (req, res) => {
    const userQuery = req.body.query;

    const connection = mysql.createConnection({
        host: 'localhost',
        user: 'root',
        password: '', //your database password
        database: 'chem_cosmetics'
    });

    connection.connect((err) => {
        if (err) {
            console.error('An error occurred while connecting to the database: ' + err.stack);
            res.status(500).json({ error: err.message });
            return;
        }

        connection.query(userQuery, (error, results) => {
            if (error) {
                console.error('An error occurred while running the query: ' + error.stack);
                res.status(500).json({ error: error.message });
                connection.end();
                return;
            }

            res.json(results);
            connection.end((err) => {
                if (err) {
                    console.error('An error occurred while closing the connection: ' + err.stack);
                }
            });
        });
    });
});

const PORT = 3000;
app.listen(PORT, () => {
    console.log(`The server is running on port ${PORT}`);
});
