open! Base;

[@deriving sexp]
type t =
  | Null
  | Boolean
  | Int8
  | Uint8
  | Int16
  | Uint16
  | Int32
  | Uint32
  | Int64
  | Uint64
  | Float16
  | Float32
  | Float64
  | Binary
  | Large_binary
  | Utf8_string
  | Large_utf8_string
  | Decimal128({
      precision: int,
      scale: int,
    })
  | Fixed_width_binary({bytes: int})
  | Date32([ | `days])
  | Date64([ | `milliseconds])
  | Time32([ | `seconds | `milliseconds])
  | Time64([ | `microseconds | `nanoseconds])
  | Timestamp({
      precision: [ | `seconds | `milliseconds | `microseconds | `nanoseconds],
      timezone: string,
    })
  | Duration([ | `seconds | `milliseconds | `microseconds | `nanoseconds])
  | Interval([ | `months | `days_time])
  | Struct
  | Map
  | Unknown(string);

let of_cstring: string => t;
