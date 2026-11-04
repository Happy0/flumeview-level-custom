const METADATA_KEY = "\x00";

function createMetadataUpdateBatchItem(version, seq) {
  return {
    key: METADATA_KEY,
    value: { version: version, since: seq },
    valueEncoding: "json",
    keyEncoding: "utf8",
    type: "put",
  };
}

module.exports = {
  METADATA_KEY,
  createMetadataUpdateItem: createMetadataUpdateBatchItem,
};
