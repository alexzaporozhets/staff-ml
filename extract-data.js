let fs = require('fs');
let readline = require('readline');
let stream = require('stream');
let MongoClient = require('mongodb').MongoClient;

const DEBUG = false;
const userIds = {};

// remove user_id form the data
function removeUserId (data){
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

function normalizeScreenshotData(data) {

  let result = {
    'user_id': (typeof data['_id']['user_id'] === 'object') ? data['_id']['user_id']['$numberLong'] : data['_id']['user_id'],
    'date': new Date(data['date']['$date']),
    'mouse': data['mousemovements'],
    'keyboard': data['keystrokes'],

    // convert milliseconds to seconds
    'interval': Math.floor(data['since_last'] / 1000),
  };

  // add info data (optional)
  if ('info' in data) result['info'] = data['info'];

  return result;
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
          row['t'] =  parseInt((typeof row['t'] === 'object') ? row['t']['$numberLong'] : row['t']);
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

function importUsers() {
  console.log('[Import users data] Started...');
  // insert all data about removed users
  let result = [];

  for (let prop in userIds) {
    if (DEBUG) console.log("obj." + prop + " = " + userIds[prop]);
    // insert record
    result.push(db.collection('timeline').insert(
      {"_id": parseInt(prop), "reason": userIds[prop]['reason'], "date": userIds[prop]['date']}
    ));
  }
  console.log('[Import users data] Done:', result.length);

  return Promise.all(result);
}

function importScreenshots() {
  console.log('[Import screenshots] Started...');

  return new Promise((resolve, reject) => {

    let instream = fs.createReadStream('/storage/screenshots.json');
    let outstream = new stream;
    let rl = readline.createInterface(instream, outstream);

    let result = [];

    rl.on('line', function (line) {
      // process line here
      if (line.indexOf('"uuid":null') === -1) {

        let data = normalizeScreenshotData(JSON.parse(line));
        let userId = data['user_id'];

        // check if this user was deleted
        if (userId in userIds) {
          if (DEBUG) console.log(data);

          // calcuate amount of days before a user deletion
          let daysBeforeLeave = daysBetween(data['date'], userIds[userId]['date']);

          let updateTimeline = {'$addToSet': {}};
          updateTimeline['$addToSet']['timeline.daysBeforeLeave_' + daysBeforeLeave + '.screenshots'] = removeUserId(data);

          // update record
          result.push(global.db.collection('timeline').update({_id: parseInt(userId)}, updateTimeline));
        }
      }
    });

    rl.on('close', function () {
      // do something on finish here
      console.log('[Import screenshots] Done:', result.length);
      Promise.all(result).then(resolve);
    });
  });
}

function importTimeuse() {
  console.log('[Import timeuse] Started...');

  return new Promise((resolve, reject) => {

    let instream = fs.createReadStream('/storage/timeuse.daily.json');
    let outstream = new stream;
    let rl = readline.createInterface(instream, outstream);

    let result = [];

    rl.on('line', function (line) {
      // process line here
      let data = normalizeTimeuseData(JSON.parse(line));
      let userId = data['user_id'];

      // check if this user was deleted
      if (userId in userIds) {
        if (DEBUG) console.log(data);

        // calcuate amount of days before a user deletion
        let daysBeforeLeave = daysBetween(data['date'], userIds[userId]['date']);

        let updateTimeline = {'$set': {}};
        updateTimeline['$set']['timeline.daysBeforeLeave_' + daysBeforeLeave + '.activity'] = removeUserId(data);

        // update record
        result.push(global.db.collection('timeline').update({_id: parseInt(userId)}, updateTimeline));
      }
    });

    rl.on('close', function () {
      // do something on finish here
      console.log('[Import timeuse] Done:', result.length);
      Promise.all(result).then(resolve);
    });
  });
}

// load info about Quit/Fired users
JSON.parse(fs.readFileSync('leave-logs.json')).forEach((d) => userIds[d.user_id] = {
  "reason": d.reason,
  "date": new Date(d.leave_date)
});

// connect away
MongoClient.connect('mongodb://127.0.0.1:27017/staff').then(function (db) {
  // todo: rewrite this hack
  global.db = db;

  // remove the previous data
  db.collection('timeline').remove({})
    .then(importUsers)
    .then(importTimeuse)
    .then(importScreenshots)
    .then(() => process.exit())
    .catch(error => {
      console.log(error); // Error: Not Found
    });
});

