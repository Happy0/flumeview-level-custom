const { EntryStream } = require("level-read-stream");
const toPull = require("stream-to-pull-stream");
const pull = require("pull-stream");

module.exports.old = function (db, opts) {
  const stream = new EntryStream(db, opts);
  return toPull.read1(stream);
};

module.exports.live = function (db, opts) {
  // WIP
  return pull.once(1);
};
