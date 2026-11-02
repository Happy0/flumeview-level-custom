const Obv = require('obv');
const path = require('path');
const Write = require('pull-write');
const charwise = require('charwise')
const { Level } = require('level');
const Live = require('pull-live');
const {old, live} = require('./read');

const METADATA_KEY = '\x00'

const MAX_ITEM_BUFFER_SIZE = 0;

/**
 * 
 * @param {*} version the version of the flumeview we're building. If this value differs to the value stored in the view's metadata, we
 * will rebuild the index from scratch.
 * @param {*} asyncMapper A function that takes a level database and the current flumelog item value as parameters. It should return an array of
 * batch operation objects to be written to the database. It's important that these are returned rather than inserted via the db parameter so that the
 * flumeview metadata can be updated at the same time as the new entries with the current log position atomically.
 * @param {*} filter Optional function to filter out items in the logstream by prior to mapping them to database entries
 * @returns 
 */
module.exports = function(version, asyncMapper, filter) {

    return function (log, name) {
        const dir = path.dirname(log.filename)

        const since = Obv();
        const db = createLevelDb(dir, log, name);
        const closed = false;

        startConsumingLog(db, since, version);

        let writer;

        async function mapEntry(item) {
            const items = await asyncMapper(db, item);
            return items;
        }

        function createWriter(db, streamEndCb) {
            return Write(function write(batch, cb) {
                if (closed) {
                    return cb(new Error('database closed while index was building'))
                } else if (batch.length !== 1) {
                    // For v1 we're only exposing an interface that allows one item to be processed at a time.
                    // For future versions we may consider batching optimisations
                    throw new Error("Batch should never have more or less than one item.")
                } else {
                    const item = batch[0];

                    const metadata = {
                        key: METADATA_KEY,
                        value: { version: version, since: item.seq },
                        valueEncoding: 'json',
                        keyEncoding: 'utf8',
                        type: 'put'
                    }

                    const update = mapEntry(item.value);

                    update.then(items => {
                        return db.batch([metadata, ...items]);
                    }).then(_ => {
                        since.set(item.since);
                        cb();
                    }).catch((e) => console.error(e))
                }                    

            }, MAX_ITEM_BUFFER_SIZE, streamEndCb)
        }

        function destroy() {
            since.set(-1);
            db.clear();

            return outdated;
        }

        async function startConsumingLog(db, since, currentVersion) {
            const value = await db.get(METADATA_KEY, { keyEncoding: 'utf8' });

            if (!value) {
                // No existing entry, we're consuming the log from the start
                since.set(-1)
            } else if (value.version === version) {
                // The version has not changed, resume consuming the log from where we left off
                // last time
                since.set(value.since)
            }
            else {
                // version has changed, wipe db and start over.
                destroy();
            }

        }

        return {
            // flumeview interface
            methods: { get: 'async', read: 'source' },
            since: since,
            createSink: function (cb) {
                writer = createWriter(db, cb);

                if (!filter) {
                    return writer;
                } else {
                    return pull(pull.filter(item => filter(item.value)), writer);
                }
            },
            destroy: destroy,
            close: function (cb) {
                closed = true;

                if (writer) {
                    writer.abort(() => db.close(cb(err)))
                } else {
                    db.close(cb);
                }
            },
            get: function(key, cb) {
                db.get(key).then(result => {
                    if (result === undefined) {
                        cb(result);
                    } else {
                        cb(null, result);
                    }
                })

            },
            read: function(opts) {
                return Live(function (opts) {
                    return old(db, opts);
                },
                function (opts) {
                    return live(db, opts);
                })(opts);
            }
        }
    }
}

function createLevelDb(dir, log, name) {
    if (!log.filename) {
        throw new Error('flumeview-level-custom can only be used with a file log')
    }

    return new Level(path.join(dir, name), {
        keyEncoding: charwise,
        valueEncoding: 'json'
    })
}