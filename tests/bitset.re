open Core_kernel;
open Arrow_c_api;

let bitset_to_string = bitset =>
  List.init(Valid.length(bitset), ~f=i =>
    if (Valid.get(bitset, i)) {
      '1';
    } else {
      '0';
    }
  )
  |> String.of_char_list;

let bitset_opt_to_string = (bitset, ~valid) =>
  List.init(Valid.length(bitset), ~f=i =>
    if (Valid.get(valid, i)) {
      if (Valid.get(bitset, i)) {
        '1';
      } else {
        '0';
      };
    } else {
      ' ';
    }
  )
  |> String.of_char_list;

let python_read_and_rewrite = (~filename) => {
  let (in_channel, out_channel) = Caml_unix.open_process("python");
  Out_channel.output_lines(
    out_channel,
    [
      "import os",
      "import pandas as pd",
      "import numpy as np",
      Printf.sprintf("df = pd.read_parquet('%s')", filename),
      "for col in df.columns:",
      "  print(''.join([' ' if b is None else '1' if b else '0' for b in list(df[col])]))",
      Printf.sprintf("os.remove('%s')", filename),
      Printf.sprintf("df.to_parquet('%s')", filename),
    ],
  );
  Out_channel.close(out_channel);
  let lines = In_channel.input_lines(in_channel);
  In_channel.close(in_channel);
  lines;
};

let%expect_test _ = {
  let test = (len, ~chunk_size) => {
    let filename = Caml.Filename.temp_file("test", ".parquet");
    Exn.protect(
      ~f=
        () => {
          let bitsets =
            List.init(
              32,
              ~f=_ => {
                let bitset = Valid.create_all_valid(len);
                for (i in 0 to len - 1) {
                  Valid.set(bitset, i, Random.bool());
                };
                bitset;
              },
            );

          let cols =
            List.mapi(bitsets, ~f=(i, bitset) =>
              Wrapper.Writer.bitset(bitset, ~name=Int.to_string(i))
            );

          Wrapper.Writer.write(~chunk_size, filename, ~cols);
          let py_bitsets = python_read_and_rewrite(~filename);
          let table = Parquet_reader.table(filename, ~column_idxs=[]);
          assert(List.length(py_bitsets) == 32);
          let py_bitsets = Array.of_list(py_bitsets);
          List.iteri(
            bitsets,
            ~f=(i, bitset) => {
              let py_bitset = py_bitsets[i];
              let bitset = bitset_to_string(bitset);
              let bitset' =
                Wrapper.Column.read_bitset(
                  table,
                  ~column=`Name(Int.to_string(i)),
                )
                |> bitset_to_string;

              if (String.(!=)(bitset, bitset')) {
                Stdio.printf("%s\n%s\n\n", bitset, bitset');
              };
              if (String.(!=)(bitset, py_bitset)) {
                Stdio.printf("ml: %s\npy: %s\n\n", bitset, py_bitset);
              };
            },
          );
        },
      ~finally=() => Caml.Sys.remove(filename),
    );
  };

  List.iter(
    ~f=((len, chunk_size)) => test(len, ~chunk_size),
    [
      (16, 32),
      (32, 32),
      (32, 31),
      (32, 11),
      (32, 16),
      (69, 32),
      (69, 27),
    ],
  );
  %expect
  {||};
};

let%expect_test _ = {
  let test = (len, ~chunk_size) => {
    let filename = Caml.Filename.temp_file("test", ".parquet");
    Exn.protect(
      ~f=
        () => {
          let bitset_and_valids =
            List.init(
              32,
              ~f=_ => {
                let rnd = () => {
                  let bitset = Valid.create_all_valid(len);
                  for (i in 0 to len - 1) {
                    Valid.set(bitset, i, Random.bool());
                  };
                  bitset;
                };

                (rnd(), rnd());
              },
            );

          let cols =
            List.mapi(bitset_and_valids, ~f=(i, (bitset, valid)) =>
              Wrapper.Writer.bitset_opt(
                bitset,
                ~valid,
                ~name=Int.to_string(i),
              )
            );

          Wrapper.Writer.write(~chunk_size, filename, ~cols);
          let py_bitsets = python_read_and_rewrite(~filename);
          let table = Parquet_reader.table(filename, ~column_idxs=[]);
          assert(List.length(py_bitsets) == 32);
          let py_bitsets = Array.of_list(py_bitsets);
          List.iteri(
            bitset_and_valids,
            ~f=(i, (bitset, valid)) => {
              let py_bitset = py_bitsets[i];
              let bitset = bitset_opt_to_string(bitset, ~valid);
              let (bitset', valid') =
                Wrapper.Column.read_bitset_opt(
                  table,
                  ~column=`Name(Int.to_string(i)),
                );

              let bitset' = bitset_opt_to_string(bitset', ~valid=valid');
              if (String.(!=)(bitset, bitset')) {
                Stdio.printf("%s\n%s\n\n", bitset, bitset');
              };
              if (String.(!=)(bitset, py_bitset)) {
                Stdio.printf("ml: %s\npy: %s\n\n", bitset, py_bitset);
              };
            },
          );
        },
      ~finally=() => Caml.Sys.remove(filename),
    );
  };

  List.iter(
    ~f=((len, chunk_size)) => test(len, ~chunk_size),
    [
      (16, 32),
      (32, 32),
      (32, 31),
      (32, 11),
      (32, 16),
      (69, 32),
      (69, 27),
    ],
  );
  %expect
  {||};
};

let%expect_test _ = {
  let run = len => {
    let bitset_and_valids =
      List.init(
        32,
        ~f=_ => {
          let rnd = () => {
            let bitset = Valid.create_all_valid(len);
            for (i in 0 to len - 1) {
              Valid.set(bitset, i, Random.bool());
            };
            bitset;
          };

          (rnd(), rnd());
        },
      );

    let cols =
      List.mapi(bitset_and_valids, ~f=(i, (bitset, valid)) =>
        Wrapper.Writer.bitset_opt(bitset, ~valid, ~name=Int.to_string(i))
      );

    let table =
      Wrapper.Writer.create_table(~cols)
      |> Wrapper.Table.slice(~offset=0, ~length=10000);

    List.iteri(
      bitset_and_valids,
      ~f=(i, (bitset, valid)) => {
        let bitset = bitset_opt_to_string(bitset, ~valid);
        let (bitset', valid') =
          Wrapper.Column.read_bitset_opt(
            table,
            ~column=`Name(Int.to_string(i)),
          );

        let bitset' = bitset_opt_to_string(bitset', ~valid=valid');
        if (String.(!=)(bitset, bitset')) {
          Stdio.printf("%s\n%s\n\n", bitset, bitset');
        };
      },
    );
  };

  List.iter([16, 23, 61], ~f=run);
};
