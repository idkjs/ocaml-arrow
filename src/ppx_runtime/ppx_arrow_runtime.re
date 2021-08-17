open! Base;
module C = Arrow_c_api.Column;
module W = Arrow_c_api.Writer;
module Valid = Arrow_c_api.Valid;

module type Col_intf = {
  type t;
  type elem;

  let init: int => t;
  let of_table: (Arrow_c_api.Table.t, string) => t;
  let writer_col: (t, string) => W.col;
  let get: (t, int) => elem;
  let set: (t, int, elem) => unit;
};

module Int_col: Col_intf with type elem = int = {
  type t = Bigarray.Array1.t(int64, Bigarray.int64_elt, Bigarray.c_layout);
  type elem = int;

  let init = len => Bigarray.Array1.create(Int64, C_layout, len);
  let of_table = (table, name) => C.read_i64_ba(table, ~column=`Name(name));
  let writer_col = (t, name) => W.int64_ba(t, ~name);
  let get = (t, idx) => t.{idx} |> Int64.to_int_exn;
  let set = (t, idx, v) => t.{idx} = Int64.of_int(v);
};

module Int_option_col: Col_intf with type elem = option(int) = {
  type t = (
    Bigarray.Array1.t(int64, Bigarray.int64_elt, Bigarray.c_layout),
    Valid.t,
  );
  type elem = option(int);

  let init = len => {
    let ba = Bigarray.Array1.create(Int64, C_layout, len);
    let v = Valid.create_all_valid(len);
    (ba, v);
  };

  let of_table = (table, name) =>
    C.read_i64_ba_opt(table, ~column=`Name(name));
  let writer_col = ((ba, valid), name) => W.int64_ba_opt(ba, valid, ~name);

  let get = ((ba, valid), idx) =>
    if (Valid.get(valid, idx)) {
      Some(ba.{idx} |> Int64.to_int_exn);
    } else {
      None;
    };

  let set = ((ba, valid), idx, v) =>
    switch (v) {
    | None => Valid.set(valid, idx, false)
    | Some(v) =>
      Valid.set(valid, idx, true);
      ba.{idx} = Int64.of_int(v);
    };
};

module Float_col: Col_intf with type elem = float = {
  type t = Bigarray.Array1.t(float, Bigarray.float64_elt, Bigarray.c_layout);
  type elem = float;

  let init = len => Bigarray.Array1.create(Float64, C_layout, len);
  let of_table = (table, name) => C.read_f64_ba(table, ~column=`Name(name));
  let writer_col = (t, name) => W.float64_ba(t, ~name);
  let get = (t, idx) => t.{idx};
  let set = (t, idx, v) => t.{idx} = v;
};

module Float_option_col: Col_intf with type elem = option(float) = {
  type t = (
    Bigarray.Array1.t(float, Bigarray.float64_elt, Bigarray.c_layout),
    Valid.t,
  );
  type elem = option(float);

  let init = len => {
    let ba = Bigarray.Array1.create(Float64, C_layout, len);
    let v = Valid.create_all_valid(len);
    (ba, v);
  };

  let of_table = (table, name) =>
    C.read_f64_ba_opt(table, ~column=`Name(name));
  let writer_col = ((ba, valid), name) =>
    W.float64_ba_opt(ba, valid, ~name);
  let get = ((ba, valid), idx) =>
    if (Valid.get(valid, idx)) {
      Some(ba.{idx});
    } else {
      None;
    };

  let set = ((ba, valid), idx, v) =>
    switch (v) {
    | None => Valid.set(valid, idx, false)
    | Some(v) =>
      Valid.set(valid, idx, true);
      ba.{idx} = v;
    };
};

module Bool_col: Col_intf with type elem = bool = {
  type t = Valid.t;
  type elem = bool;

  let init = Valid.create_all_valid;
  let of_table = (table, name) => C.read_bitset(table, ~column=`Name(name));
  let writer_col = (t, name) => W.bitset(t, ~name);
  let get = Valid.get;
  let set = Valid.set;
};

module Bool_option_col: Col_intf with type elem = option(bool) = {
  type t = {
    content: Valid.t,
    valid: Valid.t,
  };

  type elem = option(bool);

  let init = len => {
    content: Valid.create_all_valid(len),
    valid: Valid.create_all_valid(len),
  };

  let of_table = (table, name) => {
    let (content, valid) = C.read_bitset_opt(table, ~column=`Name(name));
    {content, valid};
  };

  let writer_col = ({content, valid}, name) =>
    W.bitset_opt(content, ~valid, ~name);

  let get = ({content, valid}, idx) =>
    if (Valid.get(valid, idx)) {
      Some(Valid.get(content, idx));
    } else {
      None;
    };

  let set = ({valid, content}, idx) =>
    fun
    | None => Valid.set(valid, idx, false)
    | Some(v) => {
        Valid.set(content, idx, v);
        Valid.set(valid, idx, true);
      };
};

module String_col: Col_intf with type elem = string = {
  type t = array(string);
  type elem = string;

  let init = len => Array.create(~len, "");
  let of_table = (table, name) => C.read_utf8(table, ~column=`Name(name));
  let writer_col = (t, name) => W.utf8(t, ~name);
  let get = (t, idx) => t[idx];
  let set = (t, idx, v) => t[idx] = v;
};

module String_option_col: Col_intf with type elem = option(string) = {
  type elem = option(string);
  type t = array(elem);

  let init = len => Array.create(~len, None);
  let of_table = (table, name) =>
    C.read_utf8_opt(table, ~column=`Name(name));
  let writer_col = (t, name) => W.utf8_opt(t, ~name);
  let get = (t, idx) => t[idx];
  let set = (t, idx, v) => t[idx] = v;
};

module Date_col: Col_intf with type elem = Core_kernel.Date.t = {
  type elem = Core_kernel.Date.t;
  type t = array(elem);

  let init = len => Array.create(~len, Core_kernel.Date.unix_epoch);
  let of_table = (table, name) => C.read_date(table, ~column=`Name(name));
  let writer_col = (t, name) => W.date(t, ~name);
  let get = (t, idx) => t[idx];
  let set = (t, idx, v) => t[idx] = v;
};

module Date_option_col: Col_intf with type elem = option(Core_kernel.Date.t) = {
  type elem = option(Core_kernel.Date.t);
  type t = array(elem);

  let init = len => Array.create(~len, None);
  let of_table = (table, name) =>
    C.read_date_opt(table, ~column=`Name(name));
  let writer_col = (t, name) => W.date_opt(t, ~name);
  let get = (t, idx) => t[idx];
  let set = (t, idx, v) => t[idx] = v;
};

module Time_ns_col: Col_intf with type elem = Core_kernel.Time_ns.t = {
  type elem = Core_kernel.Time_ns.t;
  type t = array(elem);

  let init = len => Array.create(~len, Core_kernel.Time_ns.epoch);
  let of_table = (table, name) =>
    C.read_time_ns(table, ~column=`Name(name));
  let writer_col = (t, name) => W.time_ns(t, ~name);
  let get = (t, idx) => t[idx];
  let set = (t, idx, v) => t[idx] = v;
};

module Time_ns_option_col:
  Col_intf with type elem = option(Core_kernel.Time_ns.t) = {
  type elem = option(Core_kernel.Time_ns.t);
  type t = array(elem);

  let init = len => Array.create(~len, None);
  let of_table = (table, name) =>
    C.read_time_ns_opt(table, ~column=`Name(name));
  let writer_col = (t, name) => W.time_ns_opt(t, ~name);
  let get = (t, idx) => t[idx];
  let set = (t, idx, v) => t[idx] = v;
};
