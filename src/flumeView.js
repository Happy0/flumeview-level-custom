const Obv = require("obv");
const path = require("path");
const charwise = require("charwise");
const { Level } = require("level");
const pull = require("pull-stream");
const { createPullStream } = require("./stream/read");
const { METADATA_KEY } = require("./metadata");

module.exports = function (version, createWriter, filter) {
  return function (log, name) {
    const dir = path.dirname(log.filename);

    const since = Obv();
    const db = createLevelDb(dir, log, name);
    let closed = false;

    startConsumingLog(db, since, version);

    let writer;

    function destroy(cb) {
      cb = cb || (() => {});
      since.set(-1);
      db.clear().then(cb);
    }

    async function startConsumingLog(db, since) {
      const value = await db.get(METADATA_KEY, { keyEncoding: "utf8" });

      if (!value) {
        // No existing entry, we're consuming the log from the start
        since.set(-1);
      } else if (value.version === version) {
        // The version has not changed, resume consuming the log from where we left off
        // last time
        since.set(value.since);
      } else {
        // version has changed, wipe db and start over.
        destroy(() => {});
      }
    }

    return {
      // flumeview interface
      methods: { get: "async", read: "source" },
      since: since,
      createSink: function (cb) {
        writer = createWriter(db, since, version, cb);

        if (!filter) {
          return writer;
        } else {
          return pull(
            pull.filter((item) => filter(item.value)),
            writer,
          );
        }
      },
      destroy: destroy,
      close: function (cb) {
        cb = cb || (() => {});

        closed = true;

        if (writer) {
          writer.abort(() => db.close().then(cb));
        } else {
          db.close().then(cb);
        }
      },
      get: function (key, cb) {
        db.get(key).then((result) => {
          if (result === undefined) {
            cb(result);
          } else {
            cb(null, result);
          }
        });
      },
      /**
       *
       * @param {*} opts The level DB stream key range options ( see 'level-read-stream' library ) as well as an optional 'live' parameter to indicate whether the
       * stream should stay live an emit live updates. IF live, a {sync:true} object will be emitted when the stream is about to emit
       * live values and each item will have a 'type' field (put, del) indicating whether the database operation type as well as the key
       * and value
       * @returns a pull-stream source of database entries satisfying the input range
       */
      read: function (opts) {
        return createPullStream(db, opts);
      },
    };
  };
};

function createLevelDb(dir, log, name) {
  if (!log.filename) {
    throw new Error("flumeview-level-custom can only be used with a file log");
  }

  return new Level(path.join(dir, name), {
    keyEncoding: charwise,
    valueEncoding: "json",
  });
}
