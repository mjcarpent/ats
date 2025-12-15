import util from 'util';
import axios from 'axios'
import https from 'https';
import express from 'express';
import mariadb from 'mariadb';

const httpsAgent = new https.Agent({
    keepAlive: true,       // keep the connection open
    keepAliveMsecs: 10000, // optional tuning
});

const router = express.Router();
const pool = mariadb.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    port: parseInt(process.env.DB_PORT || "3306", 10),
    database: process.env.DB_DATABASE,
    connectionLimit: parseInt(process.env.DB_CONNECTIONS || "10", 10)
});

const port = parseInt(process.env.SERVICE_PORT, 10);
const user = process.env.SERVICE_USER;
const pass = process.env.SERVICE_PASS;
const atsUrl = util.format(process.env.SERVICE_URL, port);

async function authenticate() {
    const response = await axios.post(
        atsUrl + "/auth",
        null,
        {
            auth: {
                username: user,
                password: pass,
            },
        }
    );

    // console.log("JWT: " + JSON.stringify(response.data.token));
    return response.data.token;
}

async function getCDRS() {

    try {

        const jwt = await authenticate();
        const response = await axios.get(atsUrl + "/cdrs",
            { 
                headers: {
                    Accept: "application/json",
                    Authorization: "Bearer " + jwt
                },
                httpsAgent,
                responseType: "stream",
                timeout: 0,
            }
        );
        
        response.data.on("data", chunk => {
            writeCDRS(JSON.parse(chunk.toString()));
            //console.log("CDR chunk:", chunk.toString());
        });

        response.data.on("end", () => {
            console.log("CDR stream ended");
        });

        //console.log("DATA: " + JSON.stringify(response.data));
    }
    catch (error) {
        console.error("getCDRS(): Failure executing request\n" + JSON.stringify(error));
    }
}

async function writeCDRS(data) {

    let conn = null;

    try {

        conn = await pool.getConnection();

        for (let i = 0; i < data.length; i++) {

            try {
                await conn.query('INSERT INTO cdrs(`cust_id`, `id`, `caller_id`, `seq`, `added_dt`, `start_time`, `end_time`) VALUES(?, ?, ?, ?, ?, ?, ?)',
                    [
                        data[i].cust_id, data[i].id, data[i].caller_id, data[i].seq, data[i].added_dt, data[i].start_time, data[i].end_time
                    ]
                );
            }
            catch (err) {
                if (err.code != "ER_DUP_ENTRY") {
                    console.error("writeCDRS: Failure inserting data\n" + JSON.stringify(err));
                }
            }
        }
    }
    catch (error) {
        console.error("writeCDRS: Failure inserting data\n" + JSON.stringify(error));
        // return res.status(500).send("Failure executing insert");
    }
    finally {
        if (conn) {
            conn.release();
        }
    }
}

router.get('/details', async function(req, res, next) {
    
    let conn = null;
    
    try {

        conn = await pool.getConnection();
        const result = await conn.query('SELECT * FROM cdrs ORDER BY start_time DESC;');
        res.json(JSON.parse(JSON.stringify(result, (key, value) => typeof value === "bigint" ? Number(value) : value)));
    }
    catch (error) {
        console.error("Error[GET /details]: Failure executing query\n" + JSON.stringify(error));
        return res.status(500).send("Failure executing details");
    }
    finally {
        if (conn) {
            conn.release();
        }
    }
});

router.get('/summary', async function(req, res, next) {
    
    let conn = null;
    
    try {

        const cust_id = req.query.cust_id;

        conn = await pool.getConnection();
        const result = await conn.query('SELECT id, DATE(start_time) AS call_date, COUNT(*) AS cdr_count FROM cdrs WHERE cust_id = ? GROUP BY id, DATE(start_time) ORDER BY call_date DESC;', [cust_id]);
        res.json(JSON.parse(JSON.stringify(result, (key, value) => typeof value === "bigint" ? Number(value) : value)));
    }
    catch (error) {
        console.error("Error[GET /summary]: Failure executing query\n" + JSON.stringify(error));
        return res.status(500).send("Failure executing summary");
    }
    finally {
        if (conn) {
            conn.release();
        }
    }
});

router.get('/logs', async function(req, res, next) {
    
    let conn = null;
    
    try {

        conn = await pool.getConnection();
        const result = await conn.query('SELECT * FROM cdrs  ORDER BY added_dt DESC;');
        res.json(JSON.parse(JSON.stringify(result, (key, value) => typeof value === "bigint" ? Number(value) : value)));
    }
    catch (error) {
        console.error("Error[GET /logs]: Failure executing query\n" + JSON.stringify(error));
        return res.status(500).send("Failure executing logs");
    }
    finally {
        if (conn) {
            conn.release();
        }
    }
});

/* GET home page. */
router.get('/', function(req, res, next) {
    res.render('index', { title: 'Express' });
});

getCDRS();

export default router;
// module.exports = router;