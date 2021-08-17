open Core_kernel;
open Arrow_c_api;

let create_t = index => {
  Ppx_t.x: index * 2 + 1,
  y: Float.of_int(index) |> Float.sqrt,
  z: Printf.sprintf("foobar %d", index),
  x_opt:
    if (index % 2 == 0) {
      None;
    } else {
      Some(index);
    },
  y_opt:
    if (index % 3 == 0) {
      None;
    } else {
      Some(Float.of_int(index) |> Float.exp);
    },
  z_opt:
    if (index % 3 == 1) {
      None;
    } else {
      Some(Printf.sprintf("%d-%d", index, index));
    },
};

let%expect_test _ = {
  let filename = Caml.Filename.temp_file("test", ".parquet");
  Exn.protect(
    ~f=
      () => {
        let ts = Array.init(100_000, ~f=create_t);
        let table = Ppx_t.arrow_table_of_t(ts);
        Table.write_parquet(table, filename, ~chunk_size=8192);
        let (_, num_rows) = Parquet_reader.schema_and_num_rows(filename);
        Stdio.printf("file has %d rows\n%!", num_rows);
        let parquet_reader = Parquet_reader.create(filename);
        Stdio.printf("created reader\n%!");
        let rec loop_read = () =>
          switch (Parquet_reader.next(parquet_reader)) {
          | None =>
            Stdio.printf("closing reader\n%!");
            Parquet_reader.close(parquet_reader);
          | Some(table) =>
            Stdio.printf("%d\n%!", Table.num_rows(table));
            loop_read();
          };

        loop_read();
      },
    ~finally=() => Caml.Sys.remove(filename),
  );
  %expect
  {|
    file has 100000 rows
    created reader
    65536
    34464
    closing reader
    |};
};
