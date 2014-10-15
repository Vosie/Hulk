(function(exports) {
  'use strict';
  var execSync = require('execSync');

  /**
   * @typedef {Object} TaskInfo
   * @property {string} cmd
   * @property {string[]} args
   * @property {Function} callback result callback.
   */

  function Executor() {
    this.queue = new Array();
  }

  /**
   * @param {TaskInfo} task the execution task
   */
  Executor.prototype.addTask = function(task) {
    this.queue.push(task);
  };

  Executor.prototype.removeTask = function(task) {
    var idx = this.queue.indexOf(task);
    if (idx > -1) {
      this.queue.splice(idx, 1);
      return idx;
    } else {
      return -1;
    }
  };

  Executor.prototype.start = function() {
    this._scheduleNext();
  };

  Executor.prototype._run = function() {
    var task = this.queue.pop();
    if (!task) {
      return;
    }
    try {
      // we use execSync to run the task.
      var out = execSync.exec(task.cmd + ' \'' +
        task.args.join('\' \'') + '\'').stdout;
      // We can have out when the process is exited.
      if (task.callback) {
        task.callback(out);
      }
    } catch (ex) {
      if (task.callback) {
        task.callback('error~' + ex.message);
      }
    }

    this._scheduleNext();
  };

  Executor.prototype._scheduleNext = function() {
    // use time-out to run next item.
    if (this.queue.length) {
      this.running = true;
      var self = this;
      setTimeout(function() {
        self._run();
      }, 500);
    } else {
      this.running = false;
    }
  };

  exports.Executor = Executor;
})(exports || window);
