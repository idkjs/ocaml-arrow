open! Base;

module type Intf = {
  type t;
  type elem;

  let create: unit => t;
  let append: (t, elem) => unit;
  let append_null: (~n: int=?, t) => unit;
  let append_opt: (t, option(elem)) => unit;
  let length: t => int;
  let null_count: t => int;
};

module Double = {
  include Wrapper.DoubleBuilder;

  let append_opt = (t, v) =>
    switch (v) {
    | None => append_null(t, ~n=1)
    | Some(v) => append(t, v)
    };

  let length = t => length(t) |> Int64.to_int_exn;
  let null_count = t => null_count(t) |> Int64.to_int_exn;
};

module String = {
  include Wrapper.StringBuilder;

  let append_opt = (t, v) =>
    switch (v) {
    | None => append_null(t, ~n=1)
    | Some(v) => append(t, v)
    };

  let length = t => length(t) |> Int64.to_int_exn;
  let null_count = t => null_count(t) |> Int64.to_int_exn;
};

module NativeInt = {
  include Wrapper.Int64Builder;

  let append = (t, v) => append(t, Int64.of_int(v));

  let append_opt = (t, v) =>
    switch (v) {
    | None => append_null(t, ~n=1)
    | Some(v) => append(t, v)
    };

  let length = t => length(t) |> Int64.to_int_exn;
  let null_count = t => null_count(t) |> Int64.to_int_exn;
};

module Int64 = {
  include Wrapper.Int64Builder;

  let append_opt = (t, v) =>
    switch (v) {
    | None => append_null(t, ~n=1)
    | Some(v) => append(t, v)
    };

  let length = t => length(t) |> Int64.to_int_exn;
  let null_count = t => null_count(t) |> Int64.to_int_exn;
};

let make_table = Wrapper.Builder.make_table;

module C = {
  type col('row, 'elem, 'col_type) = {
    name: string,
    get: 'row => 'elem,
    col_type: Table.col_type('col_type),
  };

  type packed_col('row) =
    | P(col('row, 'elem, 'elem)): packed_col('row)
    | O(col('row, option('elem), 'elem)): packed_col('row);

  type packed_cols('row) = list(packed_col('row));

  let c = (type a, col_type: Table.col_type(a), field) => {
    let name = Field.name(field);
    [P({name, get: Field.get(field), col_type})];
  };

  let c_opt = (type a, col_type: Table.col_type(a), field) => {
    let name = Field.name(field);
    [O({name, get: Field.get(field), col_type})];
  };

  let c_map = (type a, col_type: Table.col_type(a), field, ~f) => {
    let name = Field.name(field);
    let get = row => Field.get(field, row) |> f;
    [P({name, get, col_type})];
  };

  let c_map_opt = (type a, col_type: Table.col_type(a), field, ~f) => {
    let name = Field.name(field);
    let get = row => Field.get(field, row) |> f;
    [O({name, get, col_type})];
  };

  let get = (~suffixes, field, idx, row) => {
    let n_elems = List.length(suffixes);
    let row = Field.get(field, row);
    if (Array.length(row) != n_elems) {
      Printf.failwithf(
        "unexpected number of elements for %s: %d <> %d",
        Field.name(field),
        Array.length(row),
        n_elems,
        (),
      );
    };
    row[idx];
  };

  let c_array = (type a, col_type: Table.col_type(a), field, ~suffixes) => {
    let name = Field.name(field);
    List.mapi(
      suffixes,
      ~f=(idx, suffix) => {
        let get = get(~suffixes, field, idx);
        let name = name ++ suffix;
        P({name, get, col_type});
      },
    );
  };

  let c_array_opt = (type a, col_type: Table.col_type(a), field, ~suffixes) => {
    let name = Field.name(field);
    List.mapi(
      suffixes,
      ~f=(idx, suffix) => {
        let get = get(~suffixes, field, idx);
        let name = name ++ suffix;
        O({name, get, col_type});
      },
    );
  };

  let c_ignore = _field => [];

  let c_flatten = (~rename=`prefix, packed_cols, field) => {
    let rename =
      switch (rename) {
      | `keep => Fn.id
      | `prefix => (name => Field.name(field) ++ "_" ++ name)
      | `fn(fn) => fn
      };

    List.map(
      packed_cols,
      ~f=
        fun
        | P({name, get, col_type}) => {
            let name = rename(name);
            let get = row => Field.get(field, row) |> get;
            P({name, get, col_type});
          }
        | O({name, get, col_type}) => {
            let name = rename(name);
            let get = row => Field.get(field, row) |> get;
            O({name, get, col_type});
          },
    );
  };

  let array_to_table = (packed_cols, rows) => {
    let cols =
      List.map(
        packed_cols,
        ~f=
          fun
          | P({name, get, col_type}) =>
            Table.col(Array.map(rows, ~f=get), col_type, ~name)
          | O({name, get, col_type}) =>
            Table.col_opt(Array.map(rows, ~f=get), col_type, ~name),
      );

    Writer.create_table(~cols);
  };
};

module type Row_intf = {
  type row;

  let array_to_table: array(row) => Table.t;
};

module type Row_builder_intf = {
  type t;
  type row;

  let create: unit => t;
  let append: (t, row) => unit;
  let length: t => int;
  let reset: t => unit;
  let to_table: t => Table.t;
};

module Row = (R: Row_intf) => {
  type row = R.row;

  type t = {
    mutable data: list(row),
    mutable length: int,
  };

  let create = () => {data: [], length: 0};

  let append = (t, row) => {
    t.data = [row, ...t.data];
    t.length = t.length + 1;
  };

  let to_table = t => Array.of_list_rev(t.data) |> R.array_to_table;
  let length = t => t.length;

  let reset = t => {
    t.data = [];
    t.length = 0;
  };
};
