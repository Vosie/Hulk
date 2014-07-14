
var fs = require('fs');
var path = require('path');
var sqlite3 = require('sqlite3').verbose();
var CHECK_TABLE_NAME =
          'SELECT name FROM sqlite_master WHERE type=\'table\' AND name=?;';
var CREATE_TABLE_METDATA = 'CREATE TABLE metadata (\
                        _id         INTEGER PRIMARY KEY AUTOINCREMENT, \
                        key         TEXT NOT NULL, \
                        data        TEXT\
                      );';
var CREATE_TABLE_METDATA_INDEX = 
             'CREATE UNIQUE INDEX IF NOT EXISTS metadata_key ON metadata(key);';
var INSERT_METADATA ='INSERT INTO metadata(key, data) VALUES(?, ?);';
var QUERY_METADATA = 'SELECT key FROM metadata WHERE key = ?;';
var UPDATE_METADATA = 'UPDATE metadata SET data = ? WHERE key = ?';

var CREATE_TABLE = 'CREATE TABLE words (\
                      _id              INTEGER PRIMARY KEY AUTOINCREMENT,\
                      label            TEXT    NOT NULL,\
                      languageCode     TEXT    NOT NULL,\
                      serverID         TEXT    NOT NULL,\
                      url              TEXT    NOT NULL,\
                      latitude         TEXT,\
                      longtitude       TEXT,\
                      imageURL         TEXT,\
                      shortDesc        TEXT,\
                      category         INTEGER DEFAULT ( 0 )\
                    );';
var CREATE_INDEX =
         'CREATE UNIQUE INDEX IF NOT EXISTS serverIDIndex ON words(serverID);';
var INSERT_RECORD ='INSERT INTO words(label, languageCode, serverID, url,\
                                      latitude, longtitude, imageURL,\
                                      shortDesc, category)\
                                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);';
var QUERY_WORD = 'SELECT label FROM words WHERE serverID = ?;';
var UPDATE_RECORD = 'UPDATE words SET label = ?, languageCode = ?,\
                                      url = ?, latitude = ?, longtitude = ?,\
                                      imageURL = ?, shortDesc = ?, category = ?\
                                  WHERE serverID = ?';

// argument list: [check folder] [output folder] [category id] [category folder]
//                [db version]

if (process.argv.length < 7) {
  console.log('usage: node conv.js {check folder} {output folder} ' +
                     '{category id} {category folder name} {db version}');
  process.exit(-1);
  return;
}

function prepareFolder(folder) {
  if (folder.substr(-1, 1) !== path.sep) {
    return folder + path.sep;
  } else {
    return folder;
  }
}

var checkPath = prepareFolder(process.argv[2]);
if (!checkPath.substr(0, 2) !== './') {
  checkPath = './' + checkPath;
}
var rowJSONOutputFolder = prepareFolder(process.argv[3]);

var categoryFolder = prepareFolder(process.argv[5]);

var categoryID = parseInt(process.argv[4], 10);
var dbVersion = parseInt(process.argv[6], 10);
var languageCount = {};
var outputCount = {};

function listJSONFilesAndConvertThem(error, files) {
  var validFiles = [];
  files.forEach(function(file) {
    if (file.substr(-5, 5).toLowerCase() !== '.json') {
      return;
    }
    if (fs.lstatSync(checkPath + file, file).isFile()) {
      validFiles[validFiles.length] = {
        'path': checkPath + file,
        'filename': file
      };
    }
  });

  // to have lower memory consumption, we run it as single thread mode
  function runNext() {
    var fileObj = validFiles.pop();
    if (fileObj) {
      parseSingleJSONFile(fileObj.path, fileObj.filename, function() {
        runNext(); 
      });
    } else {
      var langCount = 0;
      console.log('================= parsed language count ==================');
      for (var key in languageCount) {
        console.log(key + ',' + languageCount[key]);
        langCount++;
      }
      console.log('================= output language count ==================');
      for (var key in outputCount) {
        console.log(key + ',' + outputCount[key])
      }
      console.log('all converted, total-language: ' + langCount);
    }
  }

  runNext();
}

function getSQLiteDB(lang, callback) {
  // path will be ensured before this function calls.
  var dbFile = rowJSONOutputFolder + lang + path.sep + categoryFolder + lang +
               '.sqlite3';
  var db = new sqlite3.Database(dbFile,
                                sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE,
                                function cb(err) {
                                  if (err) {
                                    console.error(err);
                                    process.exit(-1);
                                  } else {
                                    callback(db);
                                  }
                                });
}

function parseSingleJSONFile(file, filename, done) {
  console.log('process file: ' + file);
  var json = require(file);
  var key = filename.substr(0, 3);
  convertDataObject(json, key, function() {
    var otherLangs = json.otherLanguages;

    function runNext() {
      var otherLang = otherLangs.pop();
      if (!otherLang) {
        done();
      } else {
        otherLang['latitude'] = json['latitude'];
        otherLang['longitude'] = json['longitude'];
        otherLang['flagImageURL'] = json['flagImageURL'];
        convertDataObject(otherLang, key, runNext);
      }
    }
    runNext();
  });
}

function updateRecord(outputJSON, key, db, done) {
  db.run(UPDATE_RECORD, [outputJSON.label, outputJSON.languageCode,
                         outputJSON.url, outputJSON.latitude,
                         outputJSON.longitude, outputJSON.imageURL, outputJSON.shortDesc,
                         outputJSON.category, outputJSON.serverID],
         done);
}

function insertRecord(outputJSON, key, db, done) {
  
  db.run(INSERT_RECORD, [outputJSON.label, outputJSON.languageCode,
                         outputJSON.serverID, outputJSON.url,
                         outputJSON.latitude, outputJSON.longitude,
                         outputJSON.imageURL, outputJSON.shortDesc,
                         outputJSON.category],
         done);
}

function putRecord(outputJSON, key, db, done) {
  db.get(QUERY_WORD, [outputJSON.serverID], function(err, row) {
    if (row) {
      updateRecord(outputJSON, key, db, done);
    } else {
      insertRecord(outputJSON, key, db, done);
    }
  });
}

function updateMetadata(db, key, value, done) {
  db.run(UPDATE_METADATA, [value, key], done);
}

function insertMetadata(db, key, value, done) {
  db.run(INSERT_METADATA, [key, value], done);
}

function putMetadata(db, key, value, done) {
  db.get(QUERY_METADATA, [key], function(err, row) {
    if (row) {
      updateMetadata(db, key, value, done);
    } else {
      insertMetadata(db, key, value, done);
    }
  });
}

function constructOutputJSON(json, key) {
  var name = json.countryName ?
             json.name + ' (' + json.countryName + ')' : json.name;
  var shortDesc = json.shortDesc ? json.shortDesc.join('\n') : '';
  if (!name) {
    console.log('WikiDataError, name: ' + JSON.stringify(json));
    return;
  } else if (!json.wikiUrl) {
    console.log('WikiDataError, wikiUrl: ' + JSON.stringify(json));
    return;
  } else {
    shortDesc = shortDesc.substr(0, 200);
    return {
      'label': name,
      'languageCode': json.lang,
      'serverID': process.argv[5] + '/' + key,
      'url': json.wikiUrl,
      'latitude': json.latitude,
      'longitude': json.longitude,
      'imageURL': json.flagImageURL ? json.flagImageURL : '',
      'shortDesc': shortDesc,
      'category': categoryID
    };
  }
}

function ensureMetadata(db, version, lang, category, done) {
  db.get(CHECK_TABLE_NAME, ['metadata'], function(err, row) {
    if (err) {
      console.error('hulk, putMetadata: ' + err);
      process.exit(-1);
      return;
    }

    function putData() {
      putMetadata(db, 'version', version, function() {
        putMetadata(db, 'lang', lang, function() {
          putMetadata(db, 'category', category, done);
        });
      });
    }

    if (!row) {
      db.run(CREATE_TABLE_METDATA, function() {
        db.run(CREATE_TABLE_METDATA_INDEX, function() {
          putData();
        });  
      });
    } else {
      putData();
    }
  });
}

function putConvertedDataObject(db, json, key, outputFolder, done) {
  db.get(CHECK_TABLE_NAME, ['words'], function(err, row) {
    if (err) {
      console.error('hulk: ' + err);
      process.exit(-1);
      return;
    }
    
    var outputJSON = constructOutputJSON(json, key);
    if (!outputJSON) {
      db.close(done());
      return;
    }

    function outputData() {
      putRecord(outputJSON, key, db, function() {
        db.close(done());
      });
      fs.writeFile(outputFolder + key + '.json',
        JSON.stringify(outputJSON) + '\n'
      );
      if (outputCount[json.lang]) {
        outputCount[json.lang]++;
      } else {
        outputCount[json.lang] = 1;
      }
    }

    if (!row) {
      db.run(CREATE_TABLE, function() {
        db.run(CREATE_INDEX, function() {
          outputData();
        });  
      });
    } else {
      outputData();
    }
  });
}

function convertDataObject(json, key, done) {
  if (languageCount[json.lang]) {
    languageCount[json.lang]++;
  } else {
    languageCount[json.lang] = 1;
  }
  // ensure the lang folder which hosts category data and database.
  if (!fs.existsSync(rowJSONOutputFolder + json.lang)) {
    fs.mkdirSync(rowJSONOutputFolder + json.lang);
  }
  // ensure the lang + category folder which hosts all json data files.
  var jsonOutputFolder = rowJSONOutputFolder + json.lang + path.sep +
                         categoryFolder;
  if (!fs.existsSync(jsonOutputFolder)) {
    fs.mkdirSync(jsonOutputFolder);
  }
  getSQLiteDB(json.lang, function(db) {
    db.serialize(function() {
      ensureMetadata(db, dbVersion, json.lang, categoryFolder, function() {
        putConvertedDataObject(db, json, key, jsonOutputFolder, done);
      });
    });
  });
}

var dirs = fs.readdir(checkPath, listJSONFilesAndConvertThem);
