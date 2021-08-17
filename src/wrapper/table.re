open Base;
include Wrapper.Table;

type col_type(_) =
  | Int: col_type(int)
  | Float: col_type(float)
  | Utf8: col_type(string)
  | Date: col_type(Core_kernel.Date.t)
  | Time_ns: col_type(Core_kernel.Time_ns.t)
  | Bool: col_type(bool);

type packed_col =
  | P(col_type('a), array('a)): packed_col
  | O(col_type('a), array(option('a))): packed_col;

let col = (type a, data: array(a), type_: col_type(a), ~name) =>
  Writer.(
    switch (type_) {
    | Int => int(data, ~name)
    | Float => float(data, ~name)
    | Utf8 => utf8(data, ~name)
    | Date => date(data, ~name)
    | Time_ns => time_ns(data, ~name)
    | Bool =>
      let bs = Valid.create_all_valid(Array.length(data));
      Array.iteri(data, ~f=(i, a) =>
        if (!a) {
          Valid.set(bs, i, a);
        }
      );
      bitset(bs, ~name);
    }
  );

let col_opt = (type a, data: array(option(a)), type_: col_type(a), ~name) =>
  Writer.(
    switch (type_) {
    | Int => int_opt(data, ~name)
    | Float => float_opt(data, ~name)
    | Utf8 => utf8_opt(data, ~name)
    | Date => date_opt(data, ~name)
    | Time_ns => time_ns_opt(data, ~name)
    | Bool =>
      let bs = Valid.create_all_valid(Array.length(data));
      let valid = Valid.create_all_valid(Array.length(data));
      Array.iteri(data, ~f=(i, a) =>
        switch (a) {
        | None => Valid.set(valid, i, false)
        | Some(false) => Valid.set(bs, i, false)
        | Some(true) => ()
        }
      );
      bitset(valid, ~name);
    }
  );

let named_col = (packed_col, ~name) =>
  switch (packed_col) {
  | [@implicit_arity] P(typ_, data) => col(data, typ_, ~name)
  | [@implicit_arity] O(typ_, data) => col_opt(data, typ_, ~name)
  };

let create = cols => Writer.create_table(~cols);

let read = (type a, t, ~column, col_type: col_type(a)): array(a) =>
  switch (col_type) {
  | Int => Wrapper.Column.read_int(t, ~column)
  | Float => Wrapper.Column.read_float(t, ~column)
  | Utf8 => Wrapper.Column.read_utf8(t, ~column)
  | Date => Wrapper.Column.read_date(t, ~column)
  | Time_ns => Wrapper.Column.read_time_ns(t, ~column)
  | Bool =>
    let bs = Wrapper.Column.read_bitset(t, ~column);
    Array.init(Valid.length(bs), ~f=Valid.get(bs));
  };

let read_opt =
    (type a, t, ~column, col_type: col_type(a)): array(option(a)) =>
  switch (col_type) {
  | Int => Wrapper.Column.read_int_opt(t, ~column)
  | Float => Wrapper.Column.read_float_opt(t, ~column)
  | Utf8 => Wrapper.Column.read_utf8_opt(t, ~column)
  | Date => Wrapper.Column.read_date_opt(t, ~column)
  | Time_ns => Wrapper.Column.read_time_ns_opt(t, ~column)
  | Bool =>
    let (bs, valid) = Wrapper.Column.read_bitset_opt(t, ~column);
    Array.init(Valid.length(bs), ~f=i =>
      if (Valid.get(valid, i)) {
        Valid.get(bs, i) |> Option.some;
      } else {
        None;
      }
    );
  };
