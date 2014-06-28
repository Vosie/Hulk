if (process.argv.length < 4) {
  console.log('usage: node union-db.js {db.list} {base.sqlite3}');
  return;
}
var sqlite3 = require('sqlite3').verbose();
var fs = require('fs');

// arguments: {db.list} {base.sqlite3}
var unionListFile = fs.readFileSync(process.argv[2], {'encoding': 'utf-8'});
unionListFile = unionListFile.replace(/\r/gim, '');
var dbList = unionListFile.split('\n');

var dbBasePath = dbList.splice(0, 1)[0];

function getSQLiteDB(file, mode, callback) {
  var db = new sqlite3.Database(file, mode, function cb(err) {
    if (err) {
      console.error('Unable to open db file: ' +
         rowJSONOutputFolder + lang + '.sqlite3');
      process.exit(-1);
    } else {
      callback(db);
    }
  });
}

function openCompareeDB(name, callback) {
  getSQLiteDB(dbBasePath + name + '.sqlite3', sqlite3.OPEN_READONLY, callback);
}

function openBaseDB(callback) {
  getSQLiteDB(process.argv[3],
              sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE,
              callback);
}

function compareTwoDB(serverIDs, base, comparee, done) {
  var index = 0;
  function checkNext() {
    if (index === serverIDs.length) {
      done();
      return;
    }

    var serverID = serverIDs[index]; 
    comparee.get('SELECT serverID FROM words WHERE serverID = ?', [serverID],
                 function(err, row) {

      if (err) {
        console.error('Database get error: ' + err);
        process.exit(-1);
        return;
      }

      if (!row) {
        console.log('serverID not found: ' + serverID + ', remove it.');
        serverIDs.splice(index, 1);
        base.run('DELETE FROM words WHERE serverID = ?', serverID, checkNext);
      } else {
        index++;
        checkNext();
      }
    });
  }
  checkNext();
}

openBaseDB(function(base) {
  base.serialize(function() {
    console.log('base db opened');
    var serverIDs = [];

    function runNext() {
      console.log('left db count: ' + dbList.length);
      var item = dbList.splice(0, 1)[0];
      if (!item) {
        base.close();
        return;
      }
      console.log('compare db: ' + item);
      openCompareeDB(item, function(comparee) {
        comparee.serialize(function() {
          compareTwoDB(serverIDs, base, comparee, function() {
            comparee.close(runNext);
          });
        });
      });
    }

    base.all('SELECT serverID FROM words', [], function(err, rows) {
      for (var i = rows.length - 1; i >= 0; i--) {
        serverIDs[serverIDs.length] = rows[i]['serverID'];
      };
      console.log('all data loaded: ' + serverIDs.length);
      runNext();
    });
  });
});
