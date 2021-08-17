include  (module type of Wrapper.Table) with type t = Wrapper.Table.t;

type col_type(_) =
  | Int: col_type(int)
  | Float: col_type(float)
  | Utf8: col_type(string)
  | Date: col_type(Core_kernel.Date.t)
  | Time_ns: col_type(Core_kernel.Time_ns.t)
  | Bool: col_type(bool);

type packed_col =
  | P(col_type('a), array('a)): packed_col
  | O(col_type('a), array(option('a))): packed_col;

let create: list(Wrapper.Writer.col) => t;
let named_col: (packed_col, ~name: string) => Wrapper.Writer.col;
let col: (array('a), col_type('a), ~name: string) => Wrapper.Writer.col;
let col_opt:
  (array(option('a)), col_type('a), ~name: string) => Wrapper.Writer.col;
let read: (t, ~column: Wrapper.Column.column, col_type('a)) => array('a);
let read_opt:
  (t, ~column: Wrapper.Column.column, col_type('a)) => array(option('a));
