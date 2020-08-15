open! Base
module C = Arrow_c_api.Column
module W = Arrow_c_api.Writer

module type Col_intf = sig
  type t
  type elem

  val init : int -> t
  val of_table : Arrow_c_api.Table.t -> string -> t
  val writer_col : t -> string -> W.col
  val get : t -> int -> elem
  val set : t -> int -> elem -> unit
end

module Int_col : Col_intf with type elem = int = struct
  type t = (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t
  type elem = int

  let init len = Bigarray.Array1.create Int64 C_layout len
  let of_table table name = C.read_i64_ba table ~column:(`Name name)
  let writer_col t name = W.int64_ba t ~name
  let get t idx = t.{idx} |> Int64.to_int_exn
  let set t idx v = t.{idx} <- Int64.of_int v
end

module Float_col : Col_intf with type elem = float = struct
  type t = (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t
  type elem = float

  let init len = Bigarray.Array1.create Float64 C_layout len
  let of_table table name = C.read_f64_ba table ~column:(`Name name)
  let writer_col t name = W.float64_ba t ~name
  let get t idx = t.{idx}
  let set t idx v = t.{idx} <- v
end

module Bool_col : Col_intf with type elem = bool = struct
  module Valid = Arrow_c_api.Valid

  type t = Valid.t
  type elem = bool

  let init = Valid.create_all_valid
  let of_table table name = C.read_bitset table ~column:(`Name name)
  let writer_col t name = W.bitset t ~name
  let get = Valid.get
  let set = Valid.set
end

module String_col : Col_intf with type elem = string = struct
  type t = string array
  type elem = string

  let init len = Array.create ~len ""
  let of_table table name = C.read_utf8 table ~column:(`Name name)
  let writer_col t name = W.utf8 t ~name
  let get t idx = t.(idx)
  let set t idx v = t.(idx) <- v
end

module String_option_col : Col_intf with type elem = string option = struct
  type elem = string option
  type t = elem array

  let init len = Array.create ~len None
  let of_table table name = C.read_utf8_opt table ~column:(`Name name)
  let writer_col t name = W.utf8_opt t ~name
  let get t idx = t.(idx)
  let set t idx v = t.(idx) <- v
end

module Date_col : Col_intf with type elem = Core_kernel.Date.t = struct
  type elem = Core_kernel.Date.t
  type t = elem array

  let init len = Array.create ~len Core_kernel.Date.unix_epoch
  let of_table table name = C.read_date table ~column:(`Name name)
  let writer_col t name = W.date t ~name
  let get t idx = t.(idx)
  let set t idx v = t.(idx) <- v
end

module Date_option_col : Col_intf with type elem = Core_kernel.Date.t option = struct
  type elem = Core_kernel.Date.t option
  type t = elem array

  let init len = Array.create ~len None
  let of_table table name = C.read_date_opt table ~column:(`Name name)
  let writer_col t name = W.date_opt t ~name
  let get t idx = t.(idx)
  let set t idx v = t.(idx) <- v
end

module Time_ns_col : Col_intf with type elem = Core_kernel.Time_ns.t = struct
  type elem = Core_kernel.Time_ns.t
  type t = elem array

  let init len = Array.create ~len Core_kernel.Time_ns.epoch
  let of_table table name = C.read_time_ns table ~column:(`Name name)
  let writer_col t name = W.time_ns t ~name
  let get t idx = t.(idx)
  let set t idx v = t.(idx) <- v
end

module Time_ns_option_col : Col_intf with type elem = Core_kernel.Time_ns.t option =
struct
  type elem = Core_kernel.Time_ns.t option
  type t = elem array

  let init len = Array.create ~len None
  let of_table table name = C.read_time_ns_opt table ~column:(`Name name)
  let writer_col t name = W.time_ns_opt t ~name
  let get t idx = t.(idx)
  let set t idx v = t.(idx) <- v
end
