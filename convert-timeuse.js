let fs = require('fs');
let readline = require('readline');
let stream = require('stream');

// get the client
let mysql = require('mysql');

function normalizeTimeuseData(data) {

  let result = {
    'user_id': parseInt((typeof data['_id']['user'] === 'object') ? data['_id']['user']['$numberLong'] : data['_id']['user']),
    'date': new Date(data['_id']['date']['$date']),
    'apps': {},
    'websites': {}
  };

  // remove mongodb types
  ['apps', 'websites'].forEach(key => {
    if (key in data) {
      // loop over minutest from start of the day
      for (let prop in data[key]) {
        data[key][prop].forEach(row => {
          // "t":{ "$numberLong":"143"} OR "t":30
          let time = parseInt((typeof row['t'] === 'object') ? row['t']['$numberLong'] : row['t']);
          if (row['r'] in result[key]) {
            result[key][row['r']] = result[key][row['r']] + time;
          } else {
            result[key][row['r']] = time;
          }
        });
      }
    }
  });


  return result;
}

function sleepFor(sleepDuration) {
  var now = new Date().getTime();
  while (new Date().getTime() < now + sleepDuration) { /* do nothing */
  }
}

const DEBUG = false;

const config = {
  'leaveLogsPath': 'leave-logs.json',
  'screenshotsPath': '/storage/screenshots.json',
  'timeusePath': '/storage/timeuse.daily.json',
  'resultCollection': 'test100'
};

// create the connection
let connection = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: 'staff',
  database: 'timedoctor'
});

console.log('[Import timeuse] Started...');

let instream = fs.createReadStream(config.timeusePath);
let outstream = new stream;
let rl = readline.createInterface(instream, outstream);

let result = 0;
let succeedOps = 0;

rl.on('line', function (line) {

  // process line here
  if (line.indexOf('"$date":{"$numberLong"') === -1) {
    // process line here
    let data = normalizeTimeuseData(JSON.parse(line));

    if (DEBUG) console.log(data);
    let values = [];

    // remove mongodb types
    ['apps', 'websites'].forEach(key => {
      for (let prop in data[key]) {
        values.push([data.user_id, data.date, (key == 'apps') ? prop : null, (key == 'websites') ? prop : null, data[key][prop]]);
      }
    });
    result++;
    console.log(mysql.format('INSERT INTO `timeuse_daily` (user_id, date, app, website, time) VALUES ?', [values]));
    process.exit();
    connection.query('INSERT INTO `timeuse_daily` (user_id, date, app, website, time) VALUES ?', [values], function (error, results, fields) {
      if (error) throw error;
      // Neat!
      if (DEBUG) console.log('INSERT:', values);
      succeedOps++;
    });

  }
d});

rl.on('close', function () {
  setInterval(() => {
    if (succeedOps != result) {
      console.log('[Import timeuse] In-progress:', succeedOps, '/', result, 'days');
    } else {
      console.log('[Import timeuse] Done:', result, 'days');
      process.exit();
    }
  }, 500);
});