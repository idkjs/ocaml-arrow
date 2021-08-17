type t;

let create:
  (
    ~use_threads: bool=?,
    ~column_idxs: list(int)=?,
    ~mmap: bool=?,
    ~buffer_size: int=?,
    ~batch_size: int=?,
    string
  ) =>
  t;

let next: t => option(Table.t);
let close: t => unit;

let iter_batches:
  (
    ~use_threads: bool=?,
    ~column_idxs: list(int)=?,
    ~mmap: bool=?,
    ~buffer_size: int=?,
    ~batch_size: int=?,
    string,
    ~f: Table.t => unit
  ) =>
  unit;

let fold_batches:
  (
    ~use_threads: bool=?,
    ~column_idxs: list(int)=?,
    ~mmap: bool=?,
    ~buffer_size: int=?,
    ~batch_size: int=?,
    string,
    ~init: 'a,
    ~f: ('a, Table.t) => 'a
  ) =>
  'a;

let schema: string => Wrapper.Schema.t;
let schema_and_num_rows: string => (Wrapper.Schema.t, int);

let table:
  (
    ~only_first: int=?,
    ~use_threads: bool=?,
    ~column_idxs: list(int)=?,
    string
  ) =>
  Table.t;
