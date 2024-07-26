const express = require("express");
const axios = require("axios");
const sqlite3 = require("sqlite3").verbose();
const schedule = require("node-schedule");

const app = express();
const PORT = 4001;

const API_KEY = "c9262778a2f07d7e93b129a45d579800";
const CITIES = [
  "Delhi",
  "Mumbai",
  "Chennai",
  "Bangalore",
  "Kolkata",
  "Hyderabad",
];
const INTERVAL = "*/5 * * * *"; // Every 5 minutes

// Setup SQLite database
const db = new sqlite3.Database(":memory:");
// Creating the table
db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS weather_data (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      city TEXT,
      temp REAL,
      feels_like REAL,
      weather_condition TEXT,
      timestamp INTEGER
    )`);
});

// Inserting data
db.run(
  `INSERT INTO weather_data (city, temp, feels_like, weather_condition, timestamp)
     VALUES (?, ?, ?, ?, ?)`,
  [city, tempCelsius, feelsLikeCelsius, weatherCondition, timestamp]
);

// Fetch and store weather data
async function fetchWeatherData() {
  for (const city of CITIES) {
    const url = `https://api.openweathermap.org/data/2.5/weather?q=${city}&appid=${API_KEY}`;
    try {
      const response = await axios.get(url);
      const data = response.data;

      const tempKelvin = data.main.temp;
      const tempCelsius = tempKelvin - 273.15;
      const feelsLikeKelvin = data.main.feels_like;
      const feelsLikeCelsius = feelsLikeKelvin - 273.15;
      const weatherCondition = data.weather[0].main;
      const timestamp = data.dt;

      db.run(
        `INSERT INTO weather_data (city, temp, feels_like, weather_condition, timestamp)
         VALUES (?, ?, ?, ?, ?)`,
        [city, tempCelsius, feelsLikeCelsius, weatherCondition, timestamp],
        (err) => {
          if (err) {
            console.error(`Error inserting data for ${city}:`, err.message);
          }
        }
      );

      console.log(`Successfully fetched and stored data for ${city}`);
    } catch (error) {
      console.error(`Error fetching data for ${city}:`, error.message);
    }
  }
}

// Schedule the job to run at the specified interval
schedule.scheduleJob(INTERVAL, fetchWeatherData);

// API to get weather summaries
app.get("/weather_summary", (req, res) => {
  const query = `
      SELECT city, date(timestamp, 'unixepoch') as date,
        AVG(temp) as avg_temp, MAX(temp) as max_temp, MIN(temp) as min_temp,
        (SELECT weather_condition FROM weather_data 
         WHERE city=w.city AND date(timestamp, 'unixepoch')=date
         GROUP BY weather_condition ORDER BY COUNT(*) DESC LIMIT 1) as dominant_condition
      FROM weather_data w
      GROUP BY city, date
    `;
  db.all(query, [], (err, rows) => {
    if (err) {
      res.status(500).json({ error: err.message });
      return;
    }
    res.json(rows);
  });
});

// Start the server
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
  // Initial fetch to populate data
  fetchWeatherData();
});
