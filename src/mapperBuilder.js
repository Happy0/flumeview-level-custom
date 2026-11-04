const Write = require("pull-write");
const { createMetadataUpdateItem } = require("./metadata");

module.exports.sequentialMapper = function (asyncMapper) {
  const MAX_ITEM_BUFFER_SIZE = 0;

  return function (db, since, version, streamEndCb) {
    return Write(
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
            .catch((e) => console.error(e));
        }
      },
      null,
      MAX_ITEM_BUFFER_SIZE,
      streamEndCb,
    );
  };
};
