#!/usr/bin/env node

if (process.argv.length < 5) {
  console.log('usage: node android-lang-creator.js ' +
              '{lang.json} {output-folder} {filename}');
  process.exit(1);
}

var output = process.argv[3];
var filename = process.argv[4];
var langIdx = 0;
// load config file
var config;
var fs = require('fs');
var path = require('path');
var gt = require('./libs/google_translate.js');

if (process.argv[2][0] === path.sep ||
    process.argv[2].substr(1, 2) === (':' + path.sep)) {
  config = require(process.argv[2]);
} else {
  config = require(process.cwd() + path.sep + process.argv[2]);
}

// normalize the folder name
if (output[output.length - 1] !== path.sep) {
  output += path.sep;
}

function prepareFolder(folder) {
  if (!fs.existsSync(folder)) {
    fs.mkdirSync(folder);
  }
}

function generateStringXML(lang, data) {
  var outputFolder = output + 'values-' + lang + path.sep;
  prepareFolder(outputFolder);

  var outputData = '<?xml version="1.0" encoding="utf-8"?>\n' +
                   '<resources>\n';
  var hasData = false;
  for(var key in data) {
    outputData += '  <string name="' + key + '">' +
                  data[key].replace('<', '&lt;').replace('>', '&gt;') +
                  '</string>\n';
    hasData = true;
  }
  outputData += '</resources>\n';
  if (hasData) {
    fs.writeFileSync(outputFolder + filename + '.xml', outputData);
  } else {
    console.log('lang: ' + lang + ' does not have data...');
  }
}

function fixJSON(text, find, replaceAs) {
  var out = text;
  while(out.indexOf(find) > -1) {
    out = out.replace(find, replaceAs);
  }
  return out;
}

function processLang(lang, words, callback) {
  var wordIdx = 0;
  var data = {};
  function translate() {
    if (wordIdx >= words.length) {
      generateStringXML(lang, data);
      callback();
      return;
    }

    gt.translate(words[wordIdx].word, lang, 'tmp.json', function(success) {
      if (success) {
        var text = fs.readFileSync('tmp.json', 'UTF-8');
        text = fixJSON(text, ',,', ',');
        text = fixJSON(text, '[,', '[');
        text = fixJSON(text, '{,', '{');
        var result = JSON.parse(text);
        fs.unlinkSync('tmp.json');
        data[words[wordIdx].key] = result[0][0][0];
        console.log('get word for ' + words[wordIdx].word + ': ' +
                    result[0][0][0]);
      }
      wordIdx++;
      setTimeout(translate, 1000);
    });
  }
  translate();
}

function processNext() {
  if (langIdx >= config.langs.length) {
    return;
  }

  console.log('process ' + (langIdx + 1) + ' / ' + config.langs.length + ': ' +
              config.langs[langIdx]);
  processLang(config.langs[langIdx], config.words, function() {
    langIdx++;
    setTimeout(processNext, 1000);
  });
}

prepareFolder(output);
processNext();
