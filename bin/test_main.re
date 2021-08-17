open Base;
open Arrow_c_api;

type time = Core_kernel.Time_ns.t;

let sexp_of_time = time =>
  Core_kernel.Time_ns.to_string(time) |> sexp_of_string;

module Foobar = {
  [@deriving sexp]
  type t =
    | Foo
    | Bar
    | Foobar;

  let to_string = t => sexp_of_t(t) |> Sexp.to_string_mach;
  let of_string = s => Sexplib.Sexp.of_string(s) |> t_of_sexp;
};

[@deriving (sexp_of, fields)]
type t = {
  x: int,
  y: float,
  z: string,
  truc: Core_kernel.Date.t,
  time,
  y_opt: option(float),
  foobar: Foobar.t,
};

let (`read(read), `write(write)) =
  F.(
    read_write_fn(
      Fields.make_creator(
        ~x=i64,
        ~y=f64,
        ~z=str,
        ~truc=date,
        ~time=time_ns,
        ~y_opt=f64_opt,
        ~foobar=stringable((module Foobar)),
      ),
    )
  );

let () = {
  let base_time = Core_kernel.Time_ns.now();
  let base_date = Core_kernel.Date.of_string("2020-01-16");
  let date = Core_kernel.Date.add_days(base_date);
  let time = s => Core_kernel.Time_ns.(add(base_time, Span.of_sec(s)));
  let ts = [
    {
      x: 42,
      y: 3.14159265358979,
      z: "foo_z",
      truc: date(1),
      time: time(0.),
      y_opt: Some(1.414),
      foobar: Foo,
    },
    {
      x: 42,
      y: 3.14159265358979,
      z: "z_foo",
      truc: date(1),
      time: time(0.),
      y_opt: None,
      foobar: Bar,
    },
    {
      x: 1337,
      y: 2.71828182846,
      z: "bar",
      truc: date(0),
      time: time(123.45),
      y_opt: None,
      foobar: Foobar,
    },
    {
      x: 299792458,
      y: 6.02214e23,
      z: "foobar",
      truc: date(5),
      time: time(987654.),
      y_opt: Some(1.732),
      foobar: Foobar,
    },
  ];

  write("/tmp/abc.parquet", ts);
  write("/tmp/abc.feather", ts);
};

let () = {
  let filename =
    switch (Caml.Sys.argv) {
    | [|_exe, filename|] => filename
    | _ => Printf.failwithf("usage: %s file.parquet", Caml.Sys.argv[0], ())
    };

  let schema = Parquet_reader.schema(filename);
  Schema.sexp_of_t(schema)
  |> Sexp.to_string_hum
  |> Stdio.printf("Read schema:\n%s\n%!");
  let ts = read(filename);
  List.iteri(ts, ~f=(i, t) =>
    Stdio.printf("%d %s\n%!", i, sexp_of_t(t) |> Sexp.to_string_mach)
  );
  let schema = Feather_reader.schema("/tmp/foo.feather");
  Schema.sexp_of_t(schema)
  |> Sexp.to_string_hum
  |> Stdio.printf("Read schema:\n%s\n%!");
};
