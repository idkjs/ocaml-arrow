/* A typical use of this module goes as follows:
        {|
          type t =
            { x : int
            ; y : float
            }
          [@@deriving sexp, fields]

          let `read read, `write write =
            F.(read_write_fn (Fields.make_creator ~x:i64 ~y:f64))

          let ts = read "/path/to/filename.parquet"
          let () = write ts "/path/to/another.parquet"
        |}
   */
open! Base;

module Reader: {
  type t = list(string);
  type col_('v) = t => (((Wrapper.Table.t, int)) => 'v, t);
  type col('a, 'b, 'c, 'v) = Field.t_with_perm('a, 'b, 'c) => col_('v);

  let i64: col('a, 'b, 'c, int);
  let f64: col('a, 'b, 'c, float);
  let str: col('a, 'b, 'c, string);
  let stringable:
    (module Stringable.S with type t = 'd) => col('a, 'b, 'c, 'd);
  let date: col('a, 'b, 'c, Core_kernel.Date.t);
  let time_ns: col('a, 'b, 'c, Core_kernel.Time_ns.t);
  let bool: col('a, 'b, 'c, bool);
  let i64_opt: col('a, 'b, 'c, option(int));
  let f64_opt: col('a, 'b, 'c, option(float));
  let str_opt: col('a, 'b, 'c, option(string));
  let bool_opt: col('a, 'b, 'c, option(bool));

  let stringable_opt:
    (module Stringable.S with type t = 'd) => col('a, 'b, 'c, option('d));

  let date_opt: col('a, 'b, 'c, option(Core_kernel.Date.t));
  let time_ns_opt: col('a, 'b, 'c, option(Core_kernel.Time_ns.t));
  let map: (col('a, 'b, 'c, 'x), ~f: 'x => 'y) => col('a, 'b, 'c, 'y);
  let read: (col_('v), string) => list('v);
};

module Writer: {
  type state('a) = (
    int,
    list(unit => Wrapper.Writer.col),
    (int, 'a) => unit,
  );
  type col('a, 'b, 'c) =
    (state('a), Field.t_with_perm('b, 'a, 'c)) => state('a);

  let i64: col('a, 'b, int);
  let f64: col('a, 'b, float);
  let str: col('a, 'b, string);
  let bool: col('a, 'b, bool);

  let stringable:
    (
      (module Stringable.S with type t = 'd),
      state('a),
      Field.t_with_perm('c, 'a, 'd)
    ) =>
    state('a);

  let date: col('a, 'b, Core_kernel.Date.t);
  let time_ns: col('a, 'b, Core_kernel.Time_ns.t);
  let i64_opt: col('a, 'b, option(int));
  let f64_opt: col('a, 'b, option(float));
  let str_opt: col('a, 'b, option(string));
  let date_opt: col('a, 'b, option(Core_kernel.Date.t));
  let time_ns_opt: col('a, 'b, option(Core_kernel.Time_ns.t));
  let bool_opt: col('a, 'b, option(bool));

  let write:
    (
      (~init: state('d)) => state('d),
      ~chunk_size: int=?,
      ~compression: Compression.t=?,
      string,
      list('d)
    ) =>
    unit;
};

type t('a) =
  | Read(Reader.t)
  | Write(Writer.state('a));

type col('a, 'b, 'c) =
  (Field.t_with_perm('a, 'b, 'c), t('b)) =>
  (((Wrapper.Table.t, int)) => 'c, t('b));

let i64: col('a, 'b, int);
let f64: col('a, 'b, float);
let str: col('a, 'b, string);
let bool: col('a, 'b, bool);
let bool_opt: col('a, 'b, option(bool));
let stringable: (module Stringable.S with type t = 'd) => col('a, 'b, 'd);
let date: col('a, 'b, Core_kernel.Date.t);
let time_ns: col('a, 'b, Core_kernel.Time_ns.t);
let i64_opt: col('a, 'b, option(int));
let f64_opt: col('a, 'b, option(float));
let str_opt: col('a, 'b, option(string));
let date_opt: col('a, 'b, option(Core_kernel.Date.t));
let time_ns_opt: col('a, 'b, option(Core_kernel.Time_ns.t));

let read_write_fn:
  (t('a) => (((Wrapper.Table.t, int)) => 'a, t('a))) =>
  (
    [ | `read(string => list('a))],
    [
      | `write(
          (
            ~chunk_size: int=?,
            ~compression: Compression.t=?,
            string,
            list('a)
          ) =>
          unit,
        )
    ],
  );
