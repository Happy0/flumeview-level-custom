var ltgt = require("ltgt");

function eq(a, b) {
  if (Buffer.isBuffer(a) && Buffer.isBuffer(b)) {
    return beq(a, b);
  } else return a === b;
}

module.exports = function post(db, opts, each) {
  if (!each) ((each = opts), (opts = {}));

  var encode =
    (opts && opts.keyEncoding && opts.keyEncoding.encode) ||
    (db.options && db.options.keyEncoding && db.options.keyEncoding.encode) ||
    function (x) {
      return x;
    };

  var _opts = ltgt.toLtgt(opts, {}, encode);

  function cmp(key) {
    return ltgt.contains(_opts, encode(key));
  }

  function onWrite(ary) {
    ary.forEach(function (op) {
      if (cmp(op.key)) each(op);
    });
  }

  db.on("write", onWrite);

  return function () {
    db.removeListener("write", onWrite);
  };
};
