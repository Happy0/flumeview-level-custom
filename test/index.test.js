var Log = require("flumelog-offset");
const FlumeviewCustom = require("../src");
const Flume = require("flumedb");
const codec = require("flumecodec");
const util = require("util");
const { rmSync } = require("fs");

describe("flumeview-level-custom flumeview builder tests", () => {
  function mapper(db, item) {
    console.log(item);

    return [{ type: "put", key: item.id, value: item.value }];
  }

  let db = null;

  describe("get", () => {

    afterEach(async () => {
      if (db) {
        console.log("Closing!")
        db.close(() => {});
        db = null;
        rmSync('/tmp/flumeview-level-custom', {recursive: true});
      }
    })

    test("Test without optional filter function parameter set", async () => {
      db = Flume(Log("/tmp/flumeview-level-custom/log2.offset", 1024, codec.json)).use(
        "flumeviewCustom",
        FlumeviewCustom(1, mapper),
      );

      const get = util.promisify(db["flumeviewCustom"].get);
      const append = util.promisify(db.append);

      await append({ id: "testaroonie", value: "testarino" });

      const result = await get('testaroonie');

      expect(result).toStrictEqual("testarino");
    });

    test("Test with optional filter function parameter set", async () => {
      const filter = (item) => item.type !== "post";

      db = Flume(Log("/tmp/flumeview-level-custom/log.offset", 1024, codec.json)).use(
        "flumeviewCustom",
        FlumeviewCustom(1, mapper, filter),
      );

      const append = util.promisify(db.append)
      const get = util.promisify(db["flumeviewCustom"].get);

      await append({ id: "testaroonie", value: "value" });
      await append({ id: "testaroonie2", value: "value2", type: "post" })
      await append({ id: "testaroonie3", value: "value3" });

      const result1 = await get('testaroonie');
      const result2 = await get('testaroonie2');
      const result3 = await get('testaroonie3');

      expect(result1).toStrictEqual('value');
      expect(result2).toStrictEqual(undefined);
      expect(result3).toStrictEqual('value3');
    });
  });

  describe('read', () => {

    test('non-live read', () => {
        
    })

  })

});
