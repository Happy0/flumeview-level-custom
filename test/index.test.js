var Log = require("flumelog-offset");
const FlumeviewCustom = require("../src");
const Flume = require("flumedb");
const codec = require("flumecodec");
const util = require("util");

describe("flumeview-level-custom flumeview builder tests", () => {
  function mapper(db, item) {
    console.log(item);

    return [{ type: "put", key: item.id, value: item.value }];
  }

  describe("get", () => {
    test("Test without optional filter function parameter set", () => {
      const db = Flume(Log("/tmp/log.offset", 1024, codec.json)).use(
        "flumeviewCustom",
        FlumeviewCustom(1, mapper),
      );

      const get = db["flumeviewCustom"].get;

      db.append({ id: "testaroonie", value: "value" }, function (err) {
        get("testaroonie", function (result) {
          expect(result).toStrictEqual("testarino");
        });
      });
    });

    test("Test with optional filter function parameter set", () => {
      const filter = (item) => item.type !== "post";

      const db = Flume(Log("/tmp/log.offset", 1024, codec.json)).use(
        "flumeviewCustom",
        FlumeviewCustom(1, mapper),
        filter,
      );

      const get = db["flumeviewCustom"].get;
      const since = db["flumeviewCustom"].since;

      db.append({ id: "testaroonie", value: "value" }, function (err) {
        get("testaroonie", function (result) {
          expect(result).toStrictEqual("testarino");
        });
      });

      db.append(
        { id: "testaroonie2", value: "value", type: "post" },
        function (err) {
          get("testaroonie2", function (result) {
            expect(result).toBeUndefined();
          });
        },
      );

      db.append(
        { id: "testaroonie3", value: "value2", type: "post" },
        function (err) {
          get("testaroonie3", function (result) {
            expect(result).toStrictEqual("value2");
          });
        },
      );

      since.once((offset) => expect(offset).toStrictEqual(3));
    });
  });

  describe('read', () => {

    test('non-live read', () => {
        
    })

  })

});
