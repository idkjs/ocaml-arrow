open Base;

module Reader = {
  type t = list(string); /* the column names */

  type col_('v) = t => (((Wrapper.Table.t, int)) => 'v, t);
  type col('a, 'b, 'c, 'v) = Field.t_with_perm('a, 'b, 'c) => col_('v);

  let with_memo = (~get_col, field_name, t) => {
    let cache_get = ref(None);
    let get = ((table, i)) => {
      let get =
        switch (cache_get^) {
        | Some(get) => get
        | None =>
          let get = get_col(table, ~column=`Name(field_name));
          cache_get := Some(get);
          get;
        };

      get(i);
    };

    (get, [field_name, ...t]);
  };

  let i64 = field =>
    with_memo(
      Field.name(field),
      ~get_col=(table, ~column) => {
        let ba = Wrapper.Column.read_i64_ba(table, ~column);
        i => Int64.to_int_exn(ba.{i});
      },
    );

  let date = field =>
    with_memo(
      Field.name(field),
      ~get_col=(table, ~column) => {
        let a = Wrapper.Column.read_date(table, ~column);
        i => a[i];
      },
    );

  let time_ns = field =>
    with_memo(
      Field.name(field),
      ~get_col=(table, ~column) => {
        let a = Wrapper.Column.read_time_ns(table, ~column);
        i => a[i];
      },
    );

  let f64 = field =>
    with_memo(
      Field.name(field),
      ~get_col=(table, ~column) => {
        let ba = Wrapper.Column.read_f64_ba(table, ~column);
        i => ba.{i};
      },
    );

  let bool = field =>
    with_memo(
      Field.name(field),
      ~get_col=(table, ~column) => {
        let bs = Wrapper.Column.read_bitset(table, ~column);
        i => Valid.get(bs, i);
      },
    );

  let str = field =>
    with_memo(
      Field.name(field),
      ~get_col=(table, ~column) => {
        let a = Wrapper.Column.read_utf8(table, ~column);
        i => a[i];
      },
    );

  let stringable = (type a, module S: Stringable.S with type t = a, field) =>
    with_memo(
      Field.name(field),
      ~get_col=(table, ~column) => {
        let a = Wrapper.Column.read_utf8(table, ~column);
        i => a[i] |> S.of_string;
      },
    );

  let i64_opt = field =>
    with_memo(
      Field.name(field),
      ~get_col=(table, ~column) => {
        let (ba, valid) = Wrapper.Column.read_i64_ba_opt(table, ~column);
        i =>
          if (Valid.get(valid, i)) {
            Some(Int64.to_int_exn(ba.{i}));
          } else {
            None;
          };
      },
    );

  let date_opt = field =>
    with_memo(
      Field.name(field),
      ~get_col=(table, ~column) => {
        let a = Wrapper.Column.read_date_opt(table, ~column);
        i => a[i];
      },
    );

  let time_ns_opt = field =>
    with_memo(
      Field.name(field),
      ~get_col=(table, ~column) => {
        let a = Wrapper.Column.read_time_ns_opt(table, ~column);
        i => a[i];
      },
    );

  let f64_opt = field =>
    with_memo(
      Field.name(field),
      ~get_col=(table, ~column) => {
        let (ba, valid) = Wrapper.Column.read_f64_ba_opt(table, ~column);
        i =>
          if (Valid.get(valid, i)) {
            Some(ba.{i});
          } else {
            None;
          };
      },
    );

  let str_opt = field =>
    with_memo(
      Field.name(field),
      ~get_col=(table, ~column) => {
        let a = Wrapper.Column.read_utf8_opt(table, ~column);
        i => a[i];
      },
    );

  let bool_opt = field =>
    with_memo(
      Field.name(field),
      ~get_col=(table, ~column) => {
        let (bs, valid) = Wrapper.Column.read_bitset_opt(table, ~column);
        i =>
          if (Valid.get(valid, i)) {
            Some(Valid.get(bs, i));
          } else {
            None;
          };
      },
    );

  let stringable_opt = (type a, module S: Stringable.S with type t = a, field) =>
    with_memo(
      Field.name(field),
      ~get_col=(table, ~column) => {
        let array = Wrapper.Column.read_utf8_opt(table, ~column);
        i => Option.map(array[i], ~f=S.of_string);
      },
    );

  let map = (col, ~f, field, t) => {
    let (get, t) = col(field, t);
    (i => get(i) |> f, t);
  };

  let read = (creator, filename) => {
    let (get_one, col_names) = creator([]);
    let table = File_reader.table(filename, ~columns=`names(col_names));
    Wrapper.Table.num_rows(table) |> List.init(~f=i => get_one((table, i)));
  };
};

module Writer = {
  module Writer = Wrapper.Writer;

  type state('a) = (
    int,
    list(unit => Wrapper.Writer.col),
    (int, 'a) => unit,
  );
  type col('a, 'b, 'c) =
    (state('a), Field.t_with_perm('b, 'a, 'c)) => state('a);

  let i64 = ((length, acc_col, acc_set), field) => {
    let ba = Bigarray.Array1.create(Int64, C_layout, length);
    let col = () => Writer.int64_ba(ba, ~name=Field.name(field));
    let set = (idx, t) => {
      ba.{idx} = Field.get(field, t) |> Int64.of_int;
      acc_set(idx, t);
    };

    (length, [col, ...acc_col], set);
  };

  let i64_opt = ((length, acc_col, acc_set), field) => {
    let ba = Bigarray.Array1.create(Int64, C_layout, length);
    let valid = Valid.create_all_valid(length);
    let col = () => Writer.int64_ba_opt(ba, valid, ~name=Field.name(field));
    let set = (idx, t) => {
      switch (Field.get(field, t)) {
      | Some(v) => ba.{idx} = Int64.of_int(v)
      | None => Valid.set(valid, idx, false)
      };
      acc_set(idx, t);
    };

    (length, [col, ...acc_col], set);
  };

  let f64 = ((length, acc_col, acc_set), field) => {
    let ba = Bigarray.Array1.create(Float64, C_layout, length);
    let col = () => Writer.float64_ba(ba, ~name=Field.name(field));
    let set = (idx, t) => {
      ba.{idx} = Field.get(field, t);
      acc_set(idx, t);
    };

    (length, [col, ...acc_col], set);
  };

  let bool = ((length, acc_col, acc_set), field) => {
    let bs = Valid.create_all_valid(length);
    let col = () => Writer.bitset(bs, ~name=Field.name(field));
    let set = (idx, t) => {
      Valid.set(bs, idx, Field.get(field, t));
      acc_set(idx, t);
    };

    (length, [col, ...acc_col], set);
  };

  let bool_opt = ((length, acc_col, acc_set), field) => {
    let bs = Valid.create_all_valid(length);
    let valid = Valid.create_all_valid(length);
    let col = () => Writer.bitset_opt(bs, ~valid, ~name=Field.name(field));
    let set = (idx, t) => {
      switch (Field.get(field, t)) {
      | Some(v) => Valid.set(bs, idx, v)
      | None => Valid.set(valid, idx, false)
      };
      acc_set(idx, t);
    };

    (length, [col, ...acc_col], set);
  };

  let f64_opt = ((length, acc_col, acc_set), field) => {
    let ba = Bigarray.Array1.create(Float64, C_layout, length);
    let valid = Valid.create_all_valid(length);
    let col = () =>
      Writer.float64_ba_opt(ba, valid, ~name=Field.name(field));
    let set = (idx, t) => {
      switch (Field.get(field, t)) {
      | Some(v) => ba.{idx} = v
      | None => Valid.set(valid, idx, false)
      };
      acc_set(idx, t);
    };

    (length, [col, ...acc_col], set);
  };

  let str = ((length, acc_col, acc_set), field) => {
    let strs = Array.create(~len=length, "");
    let col = () => Writer.utf8(strs, ~name=Field.name(field));
    let set = (idx, t) => {
      strs[idx] = Field.get(field, t);
      acc_set(idx, t);
    };

    (length, [col, ...acc_col], set);
  };

  let str_opt = ((length, acc_col, acc_set), field) => {
    let strs = Array.create(~len=length, None);
    let col = () => Writer.utf8_opt(strs, ~name=Field.name(field));
    let set = (idx, t) => {
      strs[idx] = Field.get(field, t);
      acc_set(idx, t);
    };

    (length, [col, ...acc_col], set);
  };

  let stringable =
      (
        type a,
        module S: Stringable.S with type t = a,
        (length, acc_col, acc_set),
        field,
      ) => {
    let strs = Array.create(~len=length, "");
    let col = () => Writer.utf8(strs, ~name=Field.name(field));
    let set = (idx, t) => {
      strs[idx] = Field.get(field, t) |> S.to_string;
      acc_set(idx, t);
    };

    (length, [col, ...acc_col], set);
  };

  let date = ((length, acc_col, acc_set), field) => {
    let dates = Array.create(~len=length, Core_kernel.Date.unix_epoch);
    let col = () => Writer.date(dates, ~name=Field.name(field));
    let set = (idx, t) => {
      dates[idx] = Field.get(field, t);
      acc_set(idx, t);
    };

    (length, [col, ...acc_col], set);
  };

  let date_opt = ((length, acc_col, acc_set), field) => {
    let dates = Array.create(~len=length, None);
    let col = () => Writer.date_opt(dates, ~name=Field.name(field));
    let set = (idx, t) => {
      dates[idx] = Field.get(field, t);
      acc_set(idx, t);
    };

    (length, [col, ...acc_col], set);
  };

  let time_ns = ((length, acc_col, acc_set), field) => {
    let times = Array.create(~len=length, Core_kernel.Time_ns.epoch);
    let col = () => Writer.time_ns(times, ~name=Field.name(field));
    let set = (idx, t) => {
      times[idx] = Field.get(field, t);
      acc_set(idx, t);
    };

    (length, [col, ...acc_col], set);
  };

  let time_ns_opt = ((length, acc_col, acc_set), field) => {
    let times = Array.create(~len=length, None);
    let col = () => Writer.time_ns_opt(times, ~name=Field.name(field));
    let set = (idx, t) => {
      times[idx] = Field.get(field, t);
      acc_set(idx, t);
    };

    (length, [col, ...acc_col], set);
  };

  let write = fold => {
    let () = ();
    (~chunk_size=?, ~compression=?, filename, vs) => {
      let length = List.length(vs);
      let (_length, cols, set) =
        fold(~init=(length, [], (_idx, _t) => ()));
      List.iteri(vs, ~f=set);
      let cols = List.rev_map(cols, ~f=col => col());
      Writer.write(~chunk_size?, ~compression?, filename, ~cols);
    };
  };
};

type t('a) =
  | Read(Reader.t)
  | Write(Writer.state('a));

type col('a, 'b, 'c) =
  (Field.t_with_perm('a, 'b, 'c), t('b)) =>
  (((Wrapper.Table.t, int)) => 'c, t('b));

let i64 = (field, t) =>
  switch (t) {
  | Read(reader) =>
    let (get, reader) = Reader.i64(field, reader);
    (get, Read(reader));
  | Write(writer) =>
    let writer = Writer.i64(writer, field);
    ((_ => assert(false)), Write(writer));
  };

let i64_opt = (field, t) =>
  switch (t) {
  | Read(reader) =>
    let (get, reader) = Reader.i64_opt(field, reader);
    (get, Read(reader));
  | Write(writer) =>
    let writer = Writer.i64_opt(writer, field);
    ((_ => assert(false)), Write(writer));
  };

let f64 = (field, t) =>
  switch (t) {
  | Read(reader) =>
    let (get, reader) = Reader.f64(field, reader);
    (get, Read(reader));
  | Write(writer) =>
    let writer = Writer.f64(writer, field);
    ((_ => assert(false)), Write(writer));
  };

let bool = (field, t) =>
  switch (t) {
  | Read(reader) =>
    let (get, reader) = Reader.bool(field, reader);
    (get, Read(reader));
  | Write(writer) =>
    let writer = Writer.bool(writer, field);
    ((_ => assert(false)), Write(writer));
  };

let f64_opt = (field, t) =>
  switch (t) {
  | Read(reader) =>
    let (get, reader) = Reader.f64_opt(field, reader);
    (get, Read(reader));
  | Write(writer) =>
    let writer = Writer.f64_opt(writer, field);
    ((_ => assert(false)), Write(writer));
  };

let str = (field, t) =>
  switch (t) {
  | Read(reader) =>
    let (get, reader) = Reader.str(field, reader);
    (get, Read(reader));
  | Write(writer) =>
    let writer = Writer.str(writer, field);
    ((_ => assert(false)), Write(writer));
  };

let str_opt = (field, t) =>
  switch (t) {
  | Read(reader) =>
    let (get, reader) = Reader.str_opt(field, reader);
    (get, Read(reader));
  | Write(writer) =>
    let writer = Writer.str_opt(writer, field);
    ((_ => assert(false)), Write(writer));
  };

let stringable = (type a, module S: Stringable.S with type t = a, field, t) =>
  switch (t) {
  | Read(reader) =>
    let (get, reader) = Reader.stringable((module S), field, reader);
    (get, Read(reader));
  | Write(writer) =>
    let writer = Writer.stringable((module S), writer, field);
    ((_ => assert(false)), Write(writer));
  };

let date = (field, t) =>
  switch (t) {
  | Read(reader) =>
    let (get, reader) = Reader.date(field, reader);
    (get, Read(reader));
  | Write(writer) =>
    let writer = Writer.date(writer, field);
    ((_ => assert(false)), Write(writer));
  };

let date_opt = (field, t) =>
  switch (t) {
  | Read(reader) =>
    let (get, reader) = Reader.date_opt(field, reader);
    (get, Read(reader));
  | Write(writer) =>
    let writer = Writer.date_opt(writer, field);
    ((_ => assert(false)), Write(writer));
  };

let time_ns = (field, t) =>
  switch (t) {
  | Read(reader) =>
    let (get, reader) = Reader.time_ns(field, reader);
    (get, Read(reader));
  | Write(writer) =>
    let writer = Writer.time_ns(writer, field);
    ((_ => assert(false)), Write(writer));
  };

let time_ns_opt = (field, t) =>
  switch (t) {
  | Read(reader) =>
    let (get, reader) = Reader.time_ns_opt(field, reader);
    (get, Read(reader));
  | Write(writer) =>
    let writer = Writer.time_ns_opt(writer, field);
    ((_ => assert(false)), Write(writer));
  };

let bool_opt = (field, t) =>
  switch (t) {
  | Read(reader) =>
    let (get, reader) = Reader.bool_opt(field, reader);
    (get, Read(reader));
  | Write(writer) =>
    let writer = Writer.bool_opt(writer, field);
    ((_ => assert(false)), Write(writer));
  };

let read_write_fn = creator => {
  let read = filename =>
    Reader.read(
      r =>
        switch (creator(Read(r))) {
        | (get_one, Read(col_names)) => (get_one, col_names)
        | (_, Write(_)) => assert(false)
        },
      filename,
    );

  let write = (~chunk_size=?, ~compression=?, filename, values) => {
    let length = List.length(values);
    let (_get_one, t) =
      creator([@implicit_arity] Write(length, [], (_ixd, _t) => ()));
    let (cols, set) =
      switch (t) {
      | Read(_) => assert(false)
      | [@implicit_arity] Write(_length, cols, set) => (cols, set)
      };

    List.iteri(values, ~f=set);
    let cols = List.rev_map(cols, ~f=col => col());
    Wrapper.Writer.write(~chunk_size?, ~compression?, filename, ~cols);
  };

  (`read(read), `write(write));
};
