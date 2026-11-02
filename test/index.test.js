var Log = require('flumelog-offset');
const FlumeviewCustom = require('../src');
const Flume = require('flumedb');
const codec = require('flumecodec');
const util = require('util');

describe("flumeview-level-custom flumeview builder tests", () => {
    test("Test 'get' by key function", async () => {

        function mapper(db, item) {
            console.log(item);

            return [{type: 'put', key: item.id, value: item.value, valueEncoding: 'json', keyEncoding: 'utf8'}]
        }

        const db = Flume(Log('/tmp/log.offset', 1024, codec.json))
            .use('flumeviewCustom', FlumeviewCustom(1, mapper));

        const get = db['flumeviewCustom'].get;

        db.append({id: 'testaroonie', value: 'value'}, function(err) {
            get('testaroonie', function (result) {
                expect(result).toStrictEqual('testarino');
            });
        });
        


    })
})