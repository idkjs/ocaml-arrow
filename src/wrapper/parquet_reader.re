open Base;
module P = Wrapper.Parquet_reader;

type t = P.t;

let create = P.create;
let next = P.next;
let close = P.close;

let iter_batches =
    (
      ~use_threads=?,
      ~column_idxs=?,
      ~mmap=?,
      ~buffer_size=?,
      ~batch_size=?,
      filename,
      ~f,
    ) => {
  let t =
    P.create(
      ~use_threads?,
      ~column_idxs?,
      ~mmap?,
      ~buffer_size?,
      ~batch_size?,
      filename,
    );
  Exn.protect(
    ~finally=() => close(t),
    ~f=
      () => {
        let rec loop_read = () =>
          switch (next(t)) {
          | None => ()
          | Some(table) =>
            f(table);
            loop_read();
          };

        loop_read();
      },
  );
};

let fold_batches =
    (
      ~use_threads=?,
      ~column_idxs=?,
      ~mmap=?,
      ~buffer_size=?,
      ~batch_size=?,
      filename,
      ~init,
      ~f,
    ) => {
  let t =
    P.create(
      ~use_threads?,
      ~column_idxs?,
      ~mmap?,
      ~buffer_size?,
      ~batch_size?,
      filename,
    );
  Exn.protect(
    ~finally=() => close(t),
    ~f=
      () => {
        let rec loop_read = acc =>
          switch (next(t)) {
          | None => acc
          | Some(table) => f(acc, table) |> loop_read
          };

        loop_read(init);
      },
  );
};

let schema = P.schema;
let schema_and_num_rows = P.schema_and_num_rows;
let table = P.table;
