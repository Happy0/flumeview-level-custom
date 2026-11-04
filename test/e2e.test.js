const { FlumeViewCustom, mapperBuilder } = require("../src/index");
const Flume = require("flumedb");
const util = require("util");
const pull = require("pull-stream");
const Log = require("flumelog-offset");
const codec = require("flumecodec");
const { rmSync } = require("fs");

function pullCollectPromise(source) {
  return new Promise((resolve, reject) => {
    return pull(
      source,
      pull.collect((err, result) => {
        if (result) {
          resolve(result);
        } else {
          reject(err);
        }
      }),
    );
  });
}

function chessView(flumeDb) {
  async function chessMapper(db, item) {
    // Simplified - in a really scuttlebutt environment we'd have to deal with invite acceptances / game ends arriving
    // out of order from different user's feeds

    if (item.value.content.type === "chess_invite") {
      const gameId = item.key;

      const entry = {
        id: gameId,
        inviter: item.value.author,
        invitee: item.value.content.inviting,
        white:
          item.value.content.myColor === "white"
            ? item.value.author
            : item.value.content.inviting,
        black:
          item.value.content.myColor === "black"
            ? item.value.author
            : item.value.content.inviting,
        state: "invite",
      };

      return [
        {
          type: "put",
          key: `/games/${gameId}`,
          value: entry,
        },
        {
          type: "put",
          key: `/player/inviter/${entry.inviter}/id/${gameId}`,
          value: entry,
        },
        {
          type: "put",
          key: `/player/invited/${entry.invitee}/id/${gameId}`,
          value: entry,
        },
      ];
    } else if (item.value.content.type === "chess_invite_accept") {
      const gameId = item.value.content.root;
      const existingEntry = await db.get(`/games/${gameId}`);

      existingEntry.state = "accepted";

      const deletions = [
        {
          type: "del",
          key: `/player/inviter/${existingEntry.inviter}/id/${gameId}`,
        },
        {
          type: "del",
          key: `/player/invited/${existingEntry.invitee}/id/${gameId}`,
        },
      ];

      const insertions = [
        { type: "put", key: `/games/${gameId}`, value: existingEntry },
        {
          type: "put",
          key: `/player/live_game/${existingEntry.inviter}/game/${gameId}`,
          value: existingEntry,
        },
        {
          type: "put",
          key: `/player/live_game/${existingEntry.invitee}/game/${gameId}`,
          value: existingEntry,
        },
      ];

      return [...deletions, ...insertions];
    }
  }

  const chessFlumeView = FlumeViewCustom(
    1,
    mapperBuilder.sequentialMapper(chessMapper),
  );

  const db = flumeDb.use("flumeviewCustom", chessFlumeView);

  const chessView = db["flumeviewCustom"];
  const append = util.promisify(db.append);

  return {
    append: (item) => {
      return append(item);
    },
    getOutboundInvites: function (inviter) {
      return chessView.read({
        gt: `/player/inviter/${inviter}`,
        lt: `/player/j`,
      });
    },
    getInboundInvites: function (invitee) {
      return chessView.read({
        gt: `/player/invited/${invitee}/`,
        lt: `/player/invited/${invitee}/\uffff`,
      });
    },
    getGamesInProgress: function (playerId) {
      return chessView.read({
        gt: `/player/live_game/${playerId}/`,
        lt: `/player/live_game/${playerId}/\uffff`,
      });
    },
  };
}

describe("End to end scenario", () => {
  let db;

  afterEach(async () => {
    if (db) {
      db.close(() => {});
      db = null;
      rmSync("/tmp/flumeview-level-custom", { recursive: true, force: true });
    }
  });

  test("Chess game index scenario invites (non-live)", async () => {
    db = Flume(
      Log("/tmp/flumeview-level-custom/log2.offset", 1024, codec.json),
    );

    const chess = chessView(db);

    const items = [
      {
        key: "%Zf7y1aoZ/51P5IzF6GEvXNRkBqs+X7r+hG5djhQfwpM=.sha256",
        value: {
          previous: "%EA7jGV3XotJ3qQRMwpDbswj2xfHkEFCbZtSiXfr6d6E=.sha256",
          sequence: 19942,
          author: "@RJ09Kfs3neEZPrbpbWVDxkN92x9moe3aPusOMOc4S2I=.ed25519",
          timestamp: 1791495457292,
          hash: "sha256",
          content: {
            type: "chess_invite",
            inviting: "@STgkEO48Kucd57i7x0vBqfkz/euvt2XdglwF6gZLtdk=.ed25519",
            myColor: "black",
            branch: [
              "%AxUibLnrkZrzA6bk15LpGGDepPZOR2nEn24Kql0YYho=.sha256",
              "%t/8R6p+0Hkq7vDacYwiMOoKNzRf9DD2Ukhiyhxmsroc=.sha256",
              "%01ksKwoAAkrY+bjYdP1WCgtZHHGGcZ8bXmEoYnUlyn4=.sha256",
              "%9oK3ltfnvH9/7h5sxD6Cu+IYoSXcOddTMPJ5L6dLsUM=.sha256",
              "%ldUBk14tkOXNoLECOP9BYDyCyBh4p9KvfHAIzpwcK9k=.sha256",
            ],
          },
          signature:
            "LSq6Ul3o71i7QlPANr3xEhuazhHhPyByLoCGtxv+Nhu37VeI35UOIjCXGBfUNl+9c4f54y6XRNZXS4/ctQvhAg==.sig.ed25519",
        },
      },
      {
        key: "%GpeF1QW5NEiyMDSnotDYoU/2YDFAVVVNYJDleKpnhtw=.sha256",
        value: {
          previous: "%sat8SZr1QshItA3IZK4e6221k8i+3NOf0JLH6PlgIzw=.sha256",
          sequence: 5561,
          author: "@STgkEO48Kucd57i7x0vBqfkz/euvt2XdglwF6gZLtdk=.ed25519",
          timestamp: 1793634892038,
          hash: "sha256",
          content: {
            type: "chess_invite_accept",
            root: "%Zf7y1aoZ/51P5IzF6GEvXNRkBqs+X7r+hG5djhQfwpM=.sha256",
            branch: "%Zf7y1aoZ/51P5IzF6GEvXNRkBqs+X7r+hG5djhQfwpM=.sha256",
          },
          signature:
            "S/ZvvE9VNtkcI6vMJmijlwU8kDAjKLlWhAMenA/tZQRs1MOsDnl3AySfNMAkNEIJw4pUP0//EeTUOFPQgECwCA==.sig.ed25519",
        },
        timestamp: 1793639323422,
        rts: 1793634892038,
      },
    ];

    await chess.append(items[0]);

    const source = chess.getInboundInvites(
      "@STgkEO48Kucd57i7x0vBqfkz/euvt2XdglwF6gZLtdk=.ed25519",
    );

    const result = await pullCollectPromise(source);

    expect(result.length).toStrictEqual(1);

    const invitedSource = chess.getOutboundInvites(
      "@RJ09Kfs3neEZPrbpbWVDxkN92x9moe3aPusOMOc4S2I=.ed25519",
    );
    const invitedResult = await pullCollectPromise(invitedSource);
    expect(invitedResult.length).toStrictEqual(1);

    const inProgressSource = chess.getGamesInProgress(
      "@RJ09Kfs3neEZPrbpbWVDxkN92x9moe3aPusOMOc4S2I=.ed25519",
    );
    const inProgressResult = await pullCollectPromise(inProgressSource);
    expect(inProgressResult.length).toStrictEqual(0);

    // After invite acceptance the invites should disappear and there should be an 'in progress' result
    await chess.append(items[1]);

    const invitedSource2 = chess.getInboundInvites(
      "@RJ09Kfs3neEZPrbpbWVDxkN92x9moe3aPusOMOc4S2I=.ed25519",
    );
    const invitedResult2 = await pullCollectPromise(invitedSource2);
    expect(invitedResult2.length).toStrictEqual(0);

    const inProgressSource2 = chess.getGamesInProgress(
      "@RJ09Kfs3neEZPrbpbWVDxkN92x9moe3aPusOMOc4S2I=.ed25519",
    );

    const inProgressResult2 = await pullCollectPromise(inProgressSource2);
    expect(inProgressResult2.length).toStrictEqual(1);
  });
});
