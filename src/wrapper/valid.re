open! Base;

type ba =
  Bigarray.Array1.t(int, Bigarray.int8_unsigned_elt, Bigarray.c_layout);

type t = {
  length: int,
  data: ba,
};

let of_bigarray = (data, ~length) => {data, length};

let create_all_valid = length => {
  let data =
    Bigarray.Array1.create(Int8_unsigned, C_layout, (length + 7) / 8);
  Bigarray.Array1.fill(data, 255);
  {length, data};
};

let mask = i => 1 lsl (i land 0b111);
let unmask = i => lnot(mask(i)) land 255;
let get = (t, i) => (t.data).{i / 8} land mask(i) != 0;

let set = (t, i, b) => {
  let index = i / 8;
  if (b) {
    (t.data).{index} = (t.data).{index} lor mask(i);
  } else {
    (t.data).{index} = (t.data).{index} land unmask(i);
  };
};

let length = t => t.length;

let num_true = t => {
  let res = ref(0);
  let length = t.length;
  for (byte_index in 0 to length / 8 - 1) {
    res := res^ + Int.popcount((t.data).{byte_index});
  };
  let last_byte_index = length / 8;
  let last_bits = length % 8;
  if (last_bits > 0) {
    res :=
      res^
      + Int.popcount((t.data).{last_byte_index} land (1 lsl last_bits - 1));
  };
  res^;
};

let num_false = t => length(t) - num_true(t);
let bigarray = t => t.data;

let%expect_test _ = {
  let of_bool_list = bool_list => {
    let t = create_all_valid(List.length(bool_list));
    List.iteri(bool_list, ~f=(i, b) => set(t, i, b));
    t;
  };

  let to_bool_list = t => List.init(t.length, ~f=get(t));
  let rec gen = len =>
    if (len == 0) {
      [""];
    } else {
      let strs = gen(len - 1);
      List.map(~f=(++)("0"), strs) @ List.map(~f=(++)("1"), strs);
    };

  let strs = List.init(18, ~f=gen) |> List.concat;
  List.iter(
    ~f=
      s => {
        let t =
          String.to_list(s)
          |> List.map(
               ~f=
                 fun
                 | '1' => true
                 | '0' => false
                 | _ => assert(false),
             )
          |> of_bool_list;

        let round_trip_s =
          to_bool_list(t)
          |> List.map(
               ~f=
                 fun
                 | true => "1"
                 | false => "0",
             )
          |> String.concat(~sep="");

        let num_true_bis = String.count(s, ~f=Char.(==)('1'));
        let num_false_bis = String.count(s, ~f=Char.(==)('0'));
        if (num_true(t) != num_true_bis || num_false(t) != num_false_bis) {
          Stdio.printf("%s %d %d\n", s, num_true(t), num_false(t));
        };
        if (String.(!=)(s, round_trip_s)) {
          Stdio.printf("<%s> <%s>\n%!", s, round_trip_s);
        };
      },
    strs @ ["11111111111101111111", "11101010101011111"],
  );
  %expect
  {| |};
};
