type ba =
  Bigarray.Array1.t(int, Bigarray.int8_unsigned_elt, Bigarray.c_layout);
type t;

let of_bigarray: (ba, ~length: int) => t;
let create_all_valid: int => t;
let length: t => int;
let get: (t, int) => bool;
let set: (t, int, bool) => unit;
let bigarray: t => ba;
let num_true: t => int;
let num_false: t => int;
