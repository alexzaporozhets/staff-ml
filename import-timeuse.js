let fs = require('fs');
let readline = require('readline');
let stream = require('stream');

// get the client
let mysql = require('mysql2/promise');

// remove user_id form the data
function removeUserId(data) {
  delete data['user_id'];
  return data;
}

function daysBetween(first, second) {

  // Copy date parts of the timestamps, discarding the time parts.
  var one = new Date(first.getFullYear(), first.getMonth(), first.getDate());
  var two = new Date(second.getFullYear(), second.getMonth(), second.getDate());

  // Do the math.
  var millisecondsPerDay = 1000 * 60 * 60 * 24;
  var millisBetween = two.getTime() - one.getTime();
  var days = millisBetween / millisecondsPerDay;

  // Round down.
  return Math.floor(days);
}

function normalizeTimeuseData(data) {

  let result = {
    'user_id': (typeof data['_id']['user'] === 'object') ? data['_id']['user']['$numberLong'] : data['_id']['user'],
    'date': new Date(data['_id']['date']['$date']),
  };

  // remove mongodb types
  ['apps', 'websites'].forEach(key => {
    if (key in data) {
      // loop over minutest from start of the day
      for (let prop in data[key]) {
        data[key][prop].forEach(row => {
          // "t":{ "$numberLong":"143"} OR "t":30
          row['t'] = parseInt((typeof row['t'] === 'object') ? row['t']['$numberLong'] : row['t']);
          // remove the timestamp field
          if ('timestamp' in row) delete row['timestamp'];
        });
      }
    }
    //
    result[key] = data[key];
  });


  return result;
}

const DEBUG = true;

const config = {
  'leaveLogsPath': 'leave-logs.json',
  'screenshotsPath': '/storage/screenshots.json',
  'timeusePath': '/storage/timeuse.daily.json',
  'resultCollection': 'test100'
};

// create the connection
mysql.createConnection({ host: 'localhost', user: 'root', password: 'staff', database: 'timedoctor'}).then((connection) => {
  console.log('[Import timeuse] Started...');

  let instream = fs.createReadStream(config.timeusePath);
  let outstream = new stream;
  let rl = readline.createInterface(instream, outstream);

  let result = [];

  rl.on('line', function (line) {

    // process line here
    if (line.indexOf('"$date":{"$numberLong"') === -1) {
      // process line here
      let data = normalizeTimeuseData(JSON.parse(line));
      let userId = data['user_id'];

      if (DEBUG) console.log(data);
    }
  });

  rl.on('close', function () {
    // do something on finish here
    console.log('[Import timeuse] Done:', result.length);
    Promise.all(result).then(resolve);
  });

});

