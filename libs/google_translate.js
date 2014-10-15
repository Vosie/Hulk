(function(exports) {
  var fs = require('fs');
  var execQueue = new (require('./external_executor.js').Executor)();

  var URL = 'https://translate.google.com/translate_a/single?client=t&sl=en' +
            '&dt=bd&dt=ex&dt=ld&dt=md&dt=qc&dt=rw&dt=rm&dt=ss&dt=t&dt=at' +
            '&dt=sw&ie=UTF-8&oe=UTF-8&otf=2&ssel=0&tsel=0';
  var URL_Q = '&q=';
  var URL_TL = '&tl=';
  var URL_HL = '&hl=';

  exports.__NAME__ = 'google-translate';
  exports.translate = function gt_say(text, language, targetFile, callback) {
    // we need to use %27 to replace the single quote because it confuse the
    // shell command.
    var encodedLang = encodeURIComponent(language);
    var url = URL + URL_TL + encodedLang + URL_HL + encodedLang +
              URL_Q + encodeURIComponent(text.replace('\'', '%27'));
    execQueue.addTask({
      'cmd': 'wget',
      'args': ['-q', '-U', 'Mozilla', '-O', targetFile, url],
      'callback': function() {
        if (callback) {
          if (fs.existsSync(targetFile)) {
            var stats = fs.statSync(targetFile)
            callback(stats['size'] > 10);
          } else {
            callback(false);
          }
        }
      }
    });
    execQueue.start();
  };
})(exports || window);
