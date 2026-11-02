const { EntryStream, KeyStream, ValueStream } = require("level-read-stream");
const toPull = require("stream-to-pull-stream");
const pull = require("pull-stream");
const post = require("level-post");
const pushable = require("pull-pushable");

module.exports.old = function (db, opts) {
  const stream = createStream(db, opts);
  return toPull.read1(stream);
};

module.exports.live = function (db, opts) {
  const p = pushable(function (err) {
    if (opts.onAbort) opts.onAbort(err);
    cleanup();
  });

  const cleanup = post(db, opts, function (ch) {
    if (opts.keys === false) p.push(ch.value);
    else if (opts.values === false) p.push(ch.key);
    else p.push(ch);
  });

  return p;
};

function createStream(db, opts) {
  if (opts.keys && !opts.values) {
    return new KeyStream(db, opts);
  } else if (!opts.keys && opts.values) {
    return new ValueStream(db, opts);
  } else {
    return new EntryStream(db, opts);
  }

}