open Base;

module Test1 = {
  [@deriving (arrow, sexp_of)]
  type t = {
    x: int,
    y: float,
    z: string,
  };

  let%expect_test _ = {
    let ts = [|
      {x: 42, y: 3.14159265358979, z: "foobar"},
      {x: 1337, y: 2.71828182846, z: "foo"},
      {x: 1337 - 42, y: 0.123456, z: "bar"},
    |];

    let filename = "/tmp/abc.parquet";
    arrow_write_t(ts, filename);
    let ts = arrow_read_t(filename);
    Array.iter(ts, ~f=t =>
      sexp_of_t(t) |> Sexp.to_string_mach |> Stdio.printf("%s\n%!")
    );
    %expect
    {|
    ((x 42)(y 3.14159265358979)(z foobar))
    ((x 1337)(y 2.71828182846)(z foo))
    ((x 1295)(y 0.123456)(z bar)) |};
  };
};

module Foobar = {
  [@deriving sexp_of]
  type t =
    | Foo
    | Bar
    | Foobar;

  let to_string =
    fun
    | Foo => "Foo"
    | Bar => "Bar"
    | Foobar => "FooBar";

  let of_string =
    fun
    | "Foo" => Foo
    | "Bar" => Bar
    | "FooBar" => Foobar
    | otherwise => Printf.failwithf("unknown variant %s", otherwise, ());

  let to_int_exn =
    fun
    | Foo => 1
    | Bar => 2
    | Foobar => 3;

  let of_int_exn =
    fun
    | 1 => Foo
    | 2 => Bar
    | 3 => Foobar
    | otherwise => Printf.failwithf("unknown variant %d", otherwise, ());
};

module Test2 = {
  [@deriving (arrow, sexp_of)]
  type t = {
    x: int,
    y: float,
    [@arrow.stringable]
    z: Foobar.t,
  };

  let%expect_test _ = {
    let ts = [|
      {x: 42, y: 3.14159265358979, z: Foobar},
      {x: 1337, y: 2.71828182846, z: Foo},
      {x: 1337 - 42, y: 0.123456, z: Bar},
    |];

    let filename = "/tmp/abc.parquet";
    arrow_write_t(ts, filename);
    let ts = arrow_read_t(filename);
    Array.iter(ts, ~f=t =>
      sexp_of_t(t) |> Sexp.to_string_mach |> Stdio.printf("%s\n%!")
    );
    %expect
    {|
    ((x 42)(y 3.14159265358979)(z Foobar))
    ((x 1337)(y 2.71828182846)(z Foo))
    ((x 1295)(y 0.123456)(z Bar)) |};
    let ts = Test1.arrow_read_t(filename);
    Array.iter(ts, ~f=t =>
      Test1.sexp_of_t(t) |> Sexp.to_string_mach |> Stdio.printf("%s\n%!")
    );
    %expect
    {|
      ((x 42)(y 3.14159265358979)(z FooBar))
      ((x 1337)(y 2.71828182846)(z Foo))
      ((x 1295)(y 0.123456)(z Bar)) |};
  };
};

module Test3 = {
  [@deriving (arrow, sexp_of)]
  type t = {
    [@arrow.intable]
    x: Foobar.t,
    y: float,
    [@arrow.stringable]
    z: Foobar.t,
  };

  let%expect_test _ = {
    let ts = [|
      {x: Foo, y: 3.14159265358979, z: Foobar},
      {x: Foo, y: 2.71828182846, z: Foo},
      {x: Bar, y: 0.123456, z: Bar},
    |];

    let filename = "/tmp/abc.parquet";
    arrow_write_t(ts, filename);
    let ts = arrow_read_t(filename);
    Array.iter(ts, ~f=t =>
      sexp_of_t(t) |> Sexp.to_string_mach |> Stdio.printf("%s\n%!")
    );
    %expect
    {|
    ((x Foo)(y 3.14159265358979)(z Foobar))
    ((x Foo)(y 2.71828182846)(z Foo))
    ((x Bar)(y 0.123456)(z Bar)) |};
    let ts = Test1.arrow_read_t(filename);
    Array.iter(ts, ~f=t =>
      Test1.sexp_of_t(t) |> Sexp.to_string_mach |> Stdio.printf("%s\n%!")
    );
    %expect
    {|
      ((x 1)(y 3.14159265358979)(z FooBar))
      ((x 1)(y 2.71828182846)(z Foo))
      ((x 2)(y 0.123456)(z Bar)) |};
  };
};

module Test4 = {
  module Str = {
    [@deriving sexp_of]
    type t = string;

    let of_int_exn = Int.to_string;
    let to_int_exn = Int.of_string;
  };

  [@deriving (arrow, sexp_of)]
  type t = {
    [@arrow.intable]
    x: Str.t,
    y: float,
    [@arrow.stringable]
    z: option(Foobar.t),
    [@arrow.floatable]
    fl: option(Int.t),
  };

  module Raw = {
    [@deriving (arrow, sexp_of)]
    type t = {
      x: int,
      y: float,
      z: option(string),
      fl: option(float),
    };
  };

  let%expect_test _ = {
    let ts = [|
      {x: "1234", y: 3.14159265358979, z: Some(Foobar), fl: None},
      {x: "5678", y: 2.71828182846, z: Some(Foo), fl: Some(42)},
      {x: "-123", y: 0.123456, z: None, fl: Some(-1337)},
    |];

    let filename = "/tmp/abc.parquet";
    arrow_write_t(ts, filename);
    let ts = arrow_read_t(filename);
    Array.iter(ts, ~f=t =>
      sexp_of_t(t) |> Sexp.to_string_mach |> Stdio.printf("%s\n%!")
    );
    %expect
    {|
    ((x 1234)(y 3.14159265358979)(z(Foobar))(fl()))
    ((x 5678)(y 2.71828182846)(z(Foo))(fl(42)))
    ((x -123)(y 0.123456)(z())(fl(-1337))) |};
    let raws = Raw.arrow_read_t(filename);
    Array.iter(raws, ~f=raw =>
      Raw.sexp_of_t(raw) |> Sexp.to_string_mach |> Stdio.printf("%s\n%!")
    );
    %expect
    {|
    ((x 1234)(y 3.14159265358979)(z(FooBar))(fl()))
    ((x 5678)(y 2.71828182846)(z(Foo))(fl(42)))
    ((x -123)(y 0.123456)(z())(fl(-1337))) |};
  };
};

module Test5 = {
  open Core_kernel;

  module Foo = {
    [@deriving sexp]
    type t = {
      left: string,
      right: int,
    };
  };

  [@deriving (arrow, sexp_of)]
  type t = {
    x: int,
    [@arrow.sexpable]
    y: float,
    z: string,
    [@arrow.sexpable]
    foo: Foo.t,
    [@arrow.sexpable]
    bar: (int, float),
  };

  let%expect_test _ = {
    let ts = [|
      {
        x: 42,
        y: 3.14159265358979,
        z: "foobar",
        foo: {
          left: "1 2",
          right: 5,
        },
        bar: (1, 2.),
      },
      {
        x: 1337,
        y: 2.71828182846,
        z: "foo",
        foo: {
          left: "l",
          right: 14,
        },
        bar: (3, 4.),
      },
      {
        x: 1337 - 42,
        y: 0.123456,
        z: "bar",
        foo: {
          left: "ll",
          right: 42,
        },
        bar: (5, 6.78),
      },
    |];

    let filename = "/tmp/abc.parquet";
    arrow_write_t(ts, filename);
    let ts = arrow_read_t(filename);
    Array.iter(ts, ~f=t =>
      sexp_of_t(t) |> Sexp.to_string_mach |> Stdio.printf("%s\n%!")
    );
    %expect
    {|
    ((x 42)(y 3.14159265358979)(z foobar)(foo((left"1 2")(right 5)))(bar(1 2)))
    ((x 1337)(y 2.71828182846)(z foo)(foo((left l)(right 14)))(bar(3 4)))
    ((x 1295)(y 0.123456)(z bar)(foo((left ll)(right 42)))(bar(5 6.78))) |};
  };
};
