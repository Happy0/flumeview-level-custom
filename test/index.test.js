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
    return [{ type: "put", key: item.id, value: item.value }];
  }

  let db = null;

  describe("get", () => {
    afterEach(async () => {
      if (db) {
        db.close(() => {});
        db = null;
        rmSync("/tmp/flumeview-level-custom", { recursive: true, force: true });
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
    afterEach(async () => {
      if (db) {
        db.close(() => {});
        db = null;
        rmSync("/tmp/flumeview-level-custom", { recursive: true, force: true });
      }
    });

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
              if (item.value === "value") {
                // Wait until the stream gets going so that we get this as a 'live' stream event
                append({ id: "testaroonie3", value: "value3" });
              }

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

      const result = await resultPromise;

      // One item is a 'sync' message indicating the stream is now live
      expect(result.length).toStrictEqual(4);

      expect(result[0]).toStrictEqual({ key: "testaroonie", value: "value" });
      expect(result[1]).toStrictEqual({ key: "testaroonie2", value: "value2" });
      expect(result[3]).toStrictEqual({
        key: "testaroonie3",
        value: "value3",
        type: "put",
      });
    });

    test("Read live stream (deletion)", async () => {
      function mapper(db, item) {
        const type = item.type || "put";
        return [{ type: type, key: item.id, value: item.value, type: type }];
      }

      db = Flume(
        Log("/tmp/flumeview-level-custom/log.offset", 1024, codec.json),
      ).use("flumeviewCustom", FlumeviewCustom(1, mapper));

      const append = util.promisify(db.append);
      const read = db["flumeviewCustom"].read;

      await append({ id: "testaroonie", value: "value" });
      await append({ id: "testaroonie2", value: { test: "value2" } });

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
              if (item.key === "testaroonie") {
                // Append once we're reading from the stream so that it appears once we go live
                append({ id: "testaroonie", type: "del" });
              }

              if (item.type === "del") {
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

      const result = await resultPromise;

      expect(result.length).toStrictEqual(4);

      expect(result[0]).toStrictEqual({ key: "testaroonie", value: "value" });
      expect(result[1]).toStrictEqual({
        key: "testaroonie2",
        value: { test: "value2" },
      });
      // Sync message expected inbetween
      expect(result[3]).toStrictEqual({
        key: "testaroonie",
        type: "del",
        value: undefined,
      });
    });

    test("Read live stream (update key)", () => {});

    test("Read stream (keys only)", () => {});

    test("Read stream (values only)", () => {});

    test("Read stream (live, keys only)", () => {});

    test("Read stream (live, values only)", () => {});

    test("Read stream live, custom key encoding", () => {});
  });

  describe("Offset tracking", () => {
    test("Starts from last offset reached on startup", async () => {
      db = Flume(
        Log("/tmp/flumeview-level-custom/log2.offset", 1024, codec.json),
      ).use("flumeviewCustom", FlumeviewCustom(1, mapper));

      const get = util.promisify(db["flumeviewCustom"].get);
      const append = util.promisify(db.append);

      await append({ id: "testaroonie", value: "testarino" });

      const result = await get("testaroonie");

      expect(result).toStrictEqual("testarino");

      // Append a couple of new items before mounting the view

      db.close(() => {});

      db = Flume(
        Log("/tmp/flumeview-level-custom/log2.offset", 1024, codec.json),
      );

      const append2 = util.promisify(db.append);
      await append2({ id: "testaroonie2", value: "testarino2" });
      await append2({ id: "testaroonie3", value: "testarino3" });

      db.close(() => {});

      db = Flume(
        Log("/tmp/flumeview-level-custom/log2.offset", 1024, codec.json),
      ).use("flumeviewCustom", FlumeviewCustom(1, mapper));

      const read = db["flumeviewCustom"].read;

      const source = read({
        live: false,
        gte: "testaroonie",
        values: true,
        keys: false,
      });

      // The stream will pause until the indexing has caught up with our new items (since has reached the log seq),
      // then emit all the new items
      const result2 = await new Promise((resolve, reject) => {
        const result = [];
        pull(
          source,
          pull.drain(
            (item) => {
              result.push(item);
            },
            (err) => {
              err ? reject(err) : resolve(result);
            },
          ),
        );
      });

      expect(result2.length).toStrictEqual(3);

      expect(result2[0]).toStrictEqual("testarino");
      expect(result2[1]).toStrictEqual("testarino2");
      expect(result2[2]).toStrictEqual("testarino3");
    });

    test("Rebuilds the index if the new version is greater than the current index", async () => {
      function newSchemaMapper(db, item) {
        return [
          {
            type: "put",
            key: item.id,
            value: { item: item.value, version: 2 },
          },
        ];
      }

      db = Flume(
        Log("/tmp/flumeview-level-custom/log2.offset", 1024, codec.json),
      ).use("flumeviewCustom", FlumeviewCustom(1, mapper));

      const get = util.promisify(db["flumeviewCustom"].get);
      const append = util.promisify(db.append);

      await append({ id: "testaroonie", value: "testarino" });

      const result = await get("testaroonie");

      expect(result).toStrictEqual("testarino");

      db.close(() => {});

      db = Flume(
        Log("/tmp/flumeview-level-custom/log2.offset", 1024, codec.json),
      ).use("flumeviewCustom", FlumeviewCustom(2, newSchemaMapper));

      const get2 = util.promisify(db["flumeviewCustom"].get);

      const updatedResult = await get2("testaroonie");

      expect(updatedResult).toStrictEqual({ item: "testarino", version: 2 });
    });
  });
});
