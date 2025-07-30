const snowflake = require("snowflake-sdk");
const axios = require("axios");

let connection;

function initSnowflakeConnection() {
  if (!connection) {
    connection = snowflake.createConnection({
      account: process.env.SNOWFLAKE_ACCOUNT,
      username: process.env.SNOWFLAKE_USERNAME,
      password: process.env.SNOWFLAKE_PASSWORD,
      warehouse: process.env.SNOWFLAKE_WAREHOUSE,
      database: process.env.SNOWFLAKE_DATABASE,
      schema: process.env.SNOWFLAKE_SCHEMA,
    });

    connection.connect((err) => {
      if (err) console.error("❌ Snowflake connection failed:", err.message);
      else console.log("✅ Connected to Snowflake.");
    });
  }
}

async function getSFMCAuthToken() {
  const authUrl = `https://${process.env.SFMC_SUBDOMAIN}.auth.marketingcloudapis.com/v2/token`;
  const payload = {
    grant_type: "client_credentials",
    client_id: process.env.SFMC_CLIENT_ID,
    client_secret: process.env.SFMC_CLIENT_SECRET,
  };
  const response = await axios.post(authUrl, payload);
  return response.data.access_token;
}

async function uploadToDataExtension(rows, token) {
  const url = `https://${process.env.SFMC_SUBDOMAIN}.rest.marketingcloudapis.com/hub/v1/dataevents/key:${process.env.SFMC_DE_EXTERNAL_KEY}/rowset`;

  const response = await axios.post(url, rows, {
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
  });

  return response.data;
}

export default async function handler(req, res) {
  if (req.method !== "GET") {
    return res.status(405).send("Only GET allowed");
  }

  initSnowflakeConnection();

  const sql = `SELECT SUBSCRIBERKEY, EMAIL, FIRSTNAME, LASTNAME FROM ${process.env.SNOWFLAKE_DATABASE}.${process.env.SNOWFLAKE_SCHEMA}.SUBSCRIBERS LIMIT 5`;

  connection.execute({
    sqlText: sql,
    complete: async (err, stmt, rows) => {
      if (err) {
        console.error("❌ Query error:", err.message);
        return res.status(500).send("Snowflake query failed");
      }

      const formattedRows = rows.map((row) => ({
        keys: { SubscriberKey: row.SUBSCRIBERKEY },
        values: {
          Email: row.EMAIL,
          FirstName: row.FIRSTNAME,
          LastName: row.LASTNAME,
        },
      }));

      try {
        const token = await getSFMCAuthToken();
        const result = await uploadToDataExtension(formattedRows, token);
        res.status(200).json({ success: true, inserted: formattedRows.length, result });
      } catch (apiErr) {
        console.error("❌ SFMC error:", apiErr.message);
        res.status(500).send("SFMC API error");
      }
    },
  });
}
