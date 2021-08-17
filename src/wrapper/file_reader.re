open! Base;

let unknown_suffix = filename =>
  Printf.failwithf(
    "cannot infer the file format from suffix %s (supported suffixes are csv/json/feather/parquet)",
    filename,
    (),
  );

let schema = filename =>
  switch (String.rsplit2(filename, ~on='.')) {
  | Some((_, "csv")) => Table.read_csv(filename) |> Table.schema
  | Some((_, "json")) => Table.read_json(filename) |> Table.schema
  | Some((_, "feather")) => Wrapper.Feather_reader.schema(filename)
  | Some((_, "parquet")) => Wrapper.Parquet_reader.schema(filename)
  | Some(_)
  | None => unknown_suffix(filename)
  };

let indexes = (columns, ~filename) =>
  switch (columns) {
  | `indexes(indexes) => indexes
  | `names(col_names) =>
    let schema = schema(filename);
    let col_names = Set.of_list((module String), col_names);
    List.filter_mapi(
      schema.children,
      ~f=(i, schema) => {
        let col_name = schema.Wrapper.Schema.name;
        if (Set.mem(col_names, col_name)) {
          Some(i);
        } else {
          None;
        };
      },
    );
  };

let table = (~columns=?, filename) =>
  switch (String.rsplit2(filename, ~on='.')) {
  | Some((_, "csv")) => Table.read_csv(filename)
  | Some((_, "json")) => Table.read_json(filename)
  | Some((_, "feather")) =>
    let column_idxs = Option.map(columns, ~f=indexes(~filename));
    Wrapper.Feather_reader.table(~column_idxs?, filename);
  | Some((_, "parquet")) =>
    let column_idxs = Option.map(columns, ~f=indexes(~filename));
    Wrapper.Parquet_reader.table(~column_idxs?, filename);
  | Some(_)
  | None => unknown_suffix(filename)
  };
