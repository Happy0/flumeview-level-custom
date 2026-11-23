const Write = require("pull-write");
const { createMetadataUpdateItem } = require("./metadata");
const pull = require("pull-stream");

module.exports.sequentialMapper = function (asyncMapper, filter) {
  const MAX_ITEM_BUFFER_SIZE = 0;

  return function (db, since, version, streamEndCb) {
    const writer = Write(
      function write(batch, cb) {
        if (batch.length !== 1) {
          cb(
            new Error(
              "Batch should never have more or less than one item for sequential mapper.",
            ),
          );
        } else {
          const item = batch[0];
          const metadata = createMetadataUpdateItem(version, item.seq);
          const update = asyncMapper(db, item.value);

          Promise.resolve(update)
            .then((items) => {
              return db.batch([metadata, ...items]);
            })
            .then((_) => {
              since.set(item.seq);
              cb();
            })
            .catch((e) => {
              console.error(e);
              cb(e);
            });
        }
      },
      null,
      MAX_ITEM_BUFFER_SIZE,
      streamEndCb,
    );

    const stream = pull(
      createMetadataUpdatingFilter(filter, db, since, version),
      writer,
    );

    return stream;
  };
};

/**
 * Applies the filter but updates the 'since' observable each time and periodically
 * persists the current position in the log so that we don't have to consume large parts
 * of the log on startup if something didn't match our filter
 */
function createMetadataUpdatingFilter(filter, db, since, version) {
  // Start at -1 so that we write our progress on the first item
  let count = -1;

  return pull(
    pull.asyncMap((item, cb) => {
      count = count + 1;

      const passesFilter = filter ? filter(item.value) : true;

      if (((count % 10000) === 0) && !passesFilter) {

        const metadata = createMetadataUpdateItem(version, item.seq);

        db.batch([metadata])
          .then((_) => {
            cb(null, null);
          })
          .catch((err) => {
            console.error(err);
            cb(err, null);
          });

        since.set(item.seq);
      } else if (!passesFilter) {
        since.set(item.seq);
        cb(null, null);
      } else {
        cb(null, item);
      }
    }),
    pull.filter((item) => item !== null),
  );
}
