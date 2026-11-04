const { FlumeViewCustom, mapperBuilder } = require("../src/index");
const Flume = require("flumedb");

function chessView(flumeDb) {
  async function chessMapper(db, item) {}

  const chessFlumeView = FlumeViewCustom(
    1,
    mapperBuilder.sequentialMapper(chessMapper),
  );

  const db = flumeDb.use("flumeviewCustom", chessFlumeView);

  return {
    append: db.append,
    getOutboundInvites: function (inviter) {},
    getInboundInvites: function (invitee) {},
    getGamesInProgress: function (playerId) {}
  };
}

describe("End to end scenario", () => {
  test("Chess game index scenario invites (non-live)", () => {
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
  });
});
