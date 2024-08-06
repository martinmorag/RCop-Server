require('dotenv').config();

const express = require('express');
const { google } = require('googleapis');
const bodyParser = require('body-parser');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const rateLimit = require('express-rate-limit');
const axios = require('axios');
const app = express();
const port = 5000;

app.use(cors());
app.use(bodyParser.json());

const { GOOGLE_CLIENT_EMAIL, GOOGLE_PRIVATE_KEY } = process.env;

const auth = new google.auth.JWT(
  GOOGLE_CLIENT_EMAIL,
  null,
  GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'), // Handle newlines in environment variables
  ['https://www.googleapis.com/auth/spreadsheets']
);

const sheets = google.sheets({ version: 'v4', auth });

// Helper function to convert rows to CSV format
const rowsToCsv = (rows) => {
  const escapeCsvField = (field) => {
    if (field.includes(',')) {
      return `"${field.replace(/"/g, '""')}"`;
    }
    return field;
  };

  const header = rows[0].map(escapeCsvField).join(',');
  const data = rows.slice(1).map(row => row.map(escapeCsvField).join(',')).join('\n');
  return `${header}\n${data}`;
};


const writeRateLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // Limit each IP to 100 requests per windowMs
  message: 'Too many requests from this IP, please try again later.',
});

app.get('/filter-options', async (req, res) => {
  try {
    const getRows = await sheets.spreadsheets.values.get({
      spreadsheetId: '17dDggPOc8UTg2mkSOumNjD0XD44fiEPgZiQsvoc-ONM',
      range: 'Hoja 1',
    });

    const rows = getRows.data.values;
    if (!rows || rows.length === 0) {
      return res.status(404).send('No data found in the spreadsheet');
    }


    const headerRow = rows[0];
    const roleIndex = headerRow.indexOf('Role');
    const industryIndex = headerRow.indexOf('industry');
    const countryIndex = headerRow.indexOf('country');
    const cnaeIndex = headerRow.indexOf('CNAE');

    const roles = new Set();
    const industries = new Set();
    const countries = new Set();
    const cnaes = new Set();

    rows.slice(1).forEach(row => {
      if (roleIndex !== -1) roles.add(row[roleIndex]);
      if (industryIndex !== -1) industries.add(row[industryIndex]);
      if (countryIndex !== -1) countries.add(row[countryIndex]);
      if (cnaeIndex !== -1) cnaes.add(row[cnaeIndex]);
    });

    res.json({
      roles: Array.from(roles),
      industries: Array.from(industries),
      countries: Array.from(countries),
      cnaes: Array.from(cnaes),
    });
  } catch (error) {
    console.error('Error fetching filter options:', error);
    res.status(500).send('Error fetching filter options');
  }
});

app.post('/filter-and-download', writeRateLimiter, async (req, res) => {
  const { Role, Industry, Country, CNAE } = req.body;

  try {
    const getRows = await sheets.spreadsheets.values.get({
      spreadsheetId: '17dDggPOc8UTg2mkSOumNjD0XD44fiEPgZiQsvoc-ONM',
      range: 'Hoja 1',
    });

    const rows = getRows.data.values;
    if (!rows || rows.length === 0) {
      return res.status(404).send('No data found in the spreadsheet');
    }

    const headerRow = rows[0];
    const roleIndex = headerRow.indexOf('Role');
    const industryIndex = headerRow.indexOf('industry');
    const countryIndex = headerRow.indexOf('country');
    const cnaeIndex = headerRow.indexOf('CNAE');
    const downloadCountIndex = headerRow.indexOf('DownloadCount');

    if (roleIndex === -1 || industryIndex === -1 || countryIndex === -1 || cnaeIndex === -1 || downloadCountIndex === -1) {
      return res.status(400).send('One or more columns not found');
    }

    // Filter rows based on the criteria
    const filteredRows = rows.filter((row, index) => {
      if (index === 0) return true; // Include header row
      return (
        (Role ? row[roleIndex] === Role : true) &&
        (Industry ? row[industryIndex] === Industry : true) &&
        (Country ? row[countryIndex] === Country : true) &&
        (CNAE ? row[cnaeIndex] === CNAE : true)
      );
    });

    if (filteredRows.length <= 1) {
      return res.status(404).send('No leads found matching the criteria');
    }

    // Prepare batch update requests
    const updateRequests = filteredRows.slice(1).map((row, i) => {
      const downloadCount = parseInt(row[downloadCountIndex] || '0') + 1;
      row[downloadCountIndex] = downloadCount.toString();
      return {
        range: `Hoja 1!${String.fromCharCode(65 + downloadCountIndex)}${i + 2}`, // Adjusting for header row
        values: [[downloadCount]],
      };
    });

    // Batch update
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: '17dDggPOc8UTg2mkSOumNjD0XD44fiEPgZiQsvoc-ONM',
      resource: {
        valueInputOption: 'RAW',
        data: updateRequests,
      },
    });

    // Convert filtered rows to CSV and send as a file
    const csvData = rowsToCsv(filteredRows);

    await axios.post('https://hook.us1.make.com/pq4noj170gr728pwy7n71g8758p51kro', {
      message: 'A new lead has been downloaded.',
      filters: { Role, Industry, Country, CNAE },
      timestamp: new Date().toISOString()
    });

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename=filtered_leads.csv');
    res.send(csvData);
  } catch (error) {
    console.error('Error filtering and downloading leads:', error);
    res.status(500).send('Error filtering and downloading leads');
  }
});


app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});