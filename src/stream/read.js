const { EntryStream, KeyStream, ValueStream } = require("level-read-stream");
const toPull = require("stream-to-pull-stream");
const post = require("./post");
const pushable = require("pull-pushable");
const Live = require("pull-live");

function old(db, opts) {
  const stream = createStream(db, opts);
  return toPull.read1(stream);
}

function live(db, opts) {
  const p = pushable(function (err) {
    if (opts.onAbort) opts.onAbort(err);
    cleanup();
  });

  const cleanup = post(db, opts, function (ch) {
    if (opts.keys === false) p.push(ch.value);
    else if (opts.values === false) p.push(ch.key);
    else {
      p.push({ type: ch.type, key: ch.key, value: ch.value });
    }
  });

  return p;
}

function createStream(db, opts) {
  if (opts.keys && !opts.values) {
    return new KeyStream(db, opts);
  } else if (!opts.keys && opts.values) {
    return new ValueStream(db, opts);
  } else {
    return new EntryStream(db, opts);
  }
}

module.exports.createPullStream = function (db, opts) {
  return Live(
    function (opts) {
      return old(db, opts);
    },
    function (opts) {
      return live(db, opts);
    },
  )(opts);
};
