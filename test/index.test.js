var Log = require("flumelog-offset");
const FlumeviewCustom = require("../src");
const Flume = require("flumedb");
const codec = require("flumecodec");
const util = require("util");
const { rmSync } = require("fs");
const pull = require("pull-stream");
const Aborter = require("pull-abortable");

describe("flumeview-level-custom flumeview builder tests", () => {
  function mapper(db, item) {
    console.log(item);

    return [{ type: "put", key: item.id, value: item.value }];
  }

  let db = null;

  describe("get", () => {
    afterEach(async () => {
      if (db) {
        console.log("Closing!");
        db.close(() => {});
        db = null;
        rmSync("/tmp/flumeview-level-custom", { recursive: true });
      }
    });

    test("Test without optional filter function parameter set", async () => {
      db = Flume(
        Log("/tmp/flumeview-level-custom/log2.offset", 1024, codec.json),
      ).use("flumeviewCustom", FlumeviewCustom(1, mapper));

      const get = util.promisify(db["flumeviewCustom"].get);
      const append = util.promisify(db.append);

      await append({ id: "testaroonie", value: "testarino" });

      const result = await get("testaroonie");

      expect(result).toStrictEqual("testarino");
    });

    test("Test with optional filter function parameter set", async () => {
      const filter = (item) => item.type !== "post";

      db = Flume(
        Log("/tmp/flumeview-level-custom/log.offset", 1024, codec.json),
      ).use("flumeviewCustom", FlumeviewCustom(1, mapper, filter));

      const append = util.promisify(db.append);
      const get = util.promisify(db["flumeviewCustom"].get);

      await append({ id: "testaroonie", value: "value" });
      await append({ id: "testaroonie2", value: "value2", type: "post" });
      await append({ id: "testaroonie3", value: "value3" });

      const result1 = await get("testaroonie");
      const result2 = await get("testaroonie2");
      const result3 = await get("testaroonie3");

      expect(result1).toStrictEqual("value");
      expect(result2).toStrictEqual(undefined);
      expect(result3).toStrictEqual("value3");
    });
  });

  describe("read", () => {
    test("non-live read", async () => {
      db = Flume(
        Log("/tmp/flumeview-level-custom/log.offset", 1024, codec.json),
      ).use("flumeviewCustom", FlumeviewCustom(1, mapper));

      const append = util.promisify(db.append);
      const read = db["flumeviewCustom"].read;

      await append({ id: "testaroonie", value: "value" });
      await append({ id: "testaroonie2", value: "value2", type: "post" });
      await append({ id: "testaroonie3", value: "value3" });

      const source = read({
        live: false,
        gte: "testaroonie",
        lte: "testaroonie2",
      });

      const result = await new Promise((resolve, reject) => {
        const result = [];
        pull(
          source,
          pull.drain(
            (item) => result.push(item),
            (err) => {
              err ? reject(err) : resolve(result);
            },
          ),
        );
      });

      expect(result.length).toStrictEqual(2);

      expect(result[0]).toStrictEqual({ key: "testaroonie", value: "value" });
      expect(result[1]).toStrictEqual({ key: "testaroonie2", value: "value2" });
    });

    test("Read live stream", async () => {
      db = Flume(
        Log("/tmp/flumeview-level-custom/log.offset", 1024, codec.json),
      ).use("flumeviewCustom", FlumeviewCustom(1, mapper));

      const append = util.promisify(db.append);
      const read = db["flumeviewCustom"].read;

      await append({ id: "testaroonie", value: "value" });
      await append({ id: "testaroonie2", value: "value2", type: "post" });

      const source = read({
        live: true,
        gte: "testaroonie",
      });

      const aborter = Aborter();

      const resultPromise = new Promise((resolve, reject) => {
        const result = [];
        pull(
          source,
          aborter,
          pull.drain(
            (item) => {
              if (item.value === "value3") {
                aborter.abort();
              }

              result.push(item);
            },
            (err) => {
              err ? reject(err) : resolve(result);
            },
          ),
        );
      });

      await append({ id: "testaroonie3", value: "value3" });
      const result = await resultPromise;

      expect(result.length).toStrictEqual(3);

      expect(result[0]).toStrictEqual({ key: "testaroonie", value: "value" });
      expect(result[1]).toStrictEqual({ key: "testaroonie2", value: "value2" });
      expect(result[2]).toStrictEqual({ key: "testaroonie3", value: "value3" });
    });

    test("Read live stream (deletion)", async () => {
      //   function mapper(db, item) {
      //     console.log(item);

      //     const type = item.type || 'put';
      //     return [{ type: type, key: item.id, value: item.value }];
      //   }

      // db = Flume(
      //   Log("/tmp/flumeview-level-custom/log.offset", 1024, codec.json),
      // ).use("flumeviewCustom", FlumeviewCustom(1, mapper));

      // const append = util.promisify(db.append);
      // const read = db["flumeviewCustom"].read;

      // await append({ id: "testaroonie", value: "value" });
      // await append({ id: "testaroonie2", value: "value2" });

      // const source = read({
      //   live: true,
      //   gte: "testaroonie"
      // });

      // const aborter = Aborter();

      // const resultPromise = new Promise((resolve, reject) => {
      //   const result = [];
      //   pull(
      //     source,
      //     aborter,
      //     pull.drain(
      //       (item) => {
      //         if (item.value === "value3") {
      //           aborter.abort();
      //         }

      //         result.push(item);
      //       },
      //       (err) => {
      //         err ? reject(err) : resolve(result);
      //       },
      //     ),
      //   );
      // });

      // await append({ id: "testaroonie", value: "value3", type: 'del' });
      // const result = await resultPromise;

      // expect(result.length).toStrictEqual(3);

      // expect(result[0]).toStrictEqual({ key: "testaroonie", value: "value" });
      // expect(result[1]).toStrictEqual({ key: "testaroonie2", value: "value2" });
      // expect(result[2]).toStrictEqual({ key: "testaroonie", type: "del" });
    });

    test("Read live stream (update key)", () => {});

    test("Read stream (keys only)", () => {});

    test("Read stream (values only)", () => {});
  });

  describe("Offset tracking", () => {
    test("Starts from last offset on startup", () => {});
  });
});
