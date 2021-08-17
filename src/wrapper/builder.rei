open! Core_kernel;

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

module Double: {
  include Intf with type elem := float and type t = Wrapper.DoubleBuilder.t;
};

module Int64: {
  include Intf with type elem := Int64.t and type t = Wrapper.Int64Builder.t;
};

module NativeInt: {
  include Intf with type elem := int and type t = Wrapper.Int64Builder.t;
};

module String: {
  include Intf with type elem := string and type t = Wrapper.StringBuilder.t;
};

let make_table: list((string, Wrapper.Builder.t)) => Table.t;

module C: {
  type col('row, 'elem, 'col_type) = {
    name: string,
    get: 'row => 'elem,
    col_type: Table.col_type('col_type),
  };

  type packed_col('row) =
    | P(col('row, 'elem, 'elem)): packed_col('row)
    | O(col('row, option('elem), 'elem)): packed_col('row);

  type packed_cols('row) = list(packed_col('row));

  let c:
    (Table.col_type('a), Field.t_with_perm('b, 'c, 'a)) => packed_cols('c);
  let c_opt:
    (Table.col_type('a), Field.t_with_perm('b, 'c, option('a))) =>
    packed_cols('c);

  let c_array:
    (
      Table.col_type('a),
      Field.t_with_perm('b, 'c, array('a)),
      ~suffixes: list(string)
    ) =>
    packed_cols('c);

  let c_array_opt:
    (
      Table.col_type('a),
      Field.t_with_perm('b, 'c, array(option('a))),
      ~suffixes: list(string)
    ) =>
    packed_cols('c);

  let c_map:
    (Table.col_type('a), Field.t_with_perm('b, 'c, 'd), ~f: 'd => 'a) =>
    packed_cols('c);

  let c_map_opt:
    (
      Table.col_type('a),
      Field.t_with_perm('b, 'c, 'd),
      ~f: 'd => option('a)
    ) =>
    packed_cols('c);

  let c_ignore: Field.t_with_perm('b, 'c, 'a) => packed_cols('c);

  let c_flatten:
    (
      ~rename: [ | `fn(string => string) | `keep | `prefix]=?,
      packed_cols('a),
      Field.t_with_perm('b, 'c, 'a)
    ) =>
    packed_cols('c);

  let array_to_table: (packed_cols('a), array('a)) => Table.t;
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

module Row: (R: Row_intf) => Row_builder_intf with type row = R.row;
