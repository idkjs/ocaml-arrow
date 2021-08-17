open Core_kernel;
module A = Arrow_c_api;

let debug = false;

[@deriving sexp]
type t =
  | Null
  | Int(int)
  | Float(float)
  | String(string);

let col_readers = table => {
  let schema = A.Table.schema(table);
  List.mapi(schema.children, ~f=(col_idx, {name, format, _}) =>
    switch (format) {
    | Int64 =>
      switch (A.Column.fast_read(table, col_idx)) {
      | Int64(arr) => (i => Int(Int64.to_int_exn(arr.{i})))
      | [@implicit_arity] Int64_option(ba, valid) =>
        let valid =
          A.Valid.of_bigarray(valid, ~length=Bigarray.Array1.dim(ba));
        (
          i =>
            if (A.Valid.get(valid, i)) {
              Int(Int64.to_int_exn(ba.{i}));
            } else {
              Null;
            }
        );
      | _ => assert(false)
      }
    | Float64 =>
      switch (A.Column.fast_read(table, col_idx)) {
      | Double(arr) => (i => Float(arr.{i}))
      | [@implicit_arity] Double_option(ba, valid) =>
        let valid =
          A.Valid.of_bigarray(valid, ~length=Bigarray.Array1.dim(ba));
        (
          i =>
            if (A.Valid.get(valid, i)) {
              Float(ba.{i});
            } else {
              Null;
            }
        );
      | _ => assert(false)
      }
    | Utf8_string =>
      switch (A.Column.fast_read(table, col_idx)) {
      | String(arr) => (i => String(arr[i]))
      | String_option(arr) => (
          i =>
            switch (arr[i]) {
            | None => Null
            | Some(str) => String(str)
            }
        )
      | _ => assert(false)
      }
    | dt =>
      raise_s([%message "unsupported column type"(name, dt: A.Datatype.t)])
    }
  );
};

let () = {
  let filename =
    switch (Caml.Sys.argv) {
    | [|_exe, filename|] => filename
    | _ => Printf.failwithf("usage: %s file.parquet", Caml.Sys.argv[0], ())
    };

  let prev_time = ref(Time_ns.now());
  A.Parquet_reader.iter_batches(
    filename,
    ~batch_size=8192,
    ~f=table => {
      let num_rows = A.Table.num_rows(table);
      let col_readers = col_readers(table);
      for (row_idx in 0 to num_rows - 1) {
        let values =
          List.map(col_readers, ~f=col_reader => col_reader(row_idx));
        if (debug) {
          print_s([%sexp_of: list(t)](values));
        };
      };
      let now = Time_ns.now();
      let dt = Time_ns.diff(now, prev_time^) |> Time_ns.Span.to_sec;
      let krows_per_sec = Float.of_int(num_rows) /. dt /. 1000.;
      prev_time := now;
      Stdio.printf(
        "read batch with %d rows, %.0f krows/sec\n%!",
        num_rows,
        krows_per_sec,
      );
    },
  );
};
