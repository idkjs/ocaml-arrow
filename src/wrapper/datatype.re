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

let of_cstring =
  fun
  | "n" => Null
  | "b" => Boolean
  | "c" => Int8
  | "C" => Uint8
  | "s" => Int16
  | "S" => Uint16
  | "i" => Int32
  | "I" => Uint32
  | "l" => Int64
  | "L" => Uint64
  | "e" => Float16
  | "f" => Float32
  | "g" => Float64
  | "z" => Binary
  | "Z" => Large_binary
  | "u" => Utf8_string
  | "U" => Large_utf8_string
  | "tdD" => Date32(`days)
  | "tdm" => Date64(`milliseconds)
  | "tts" => Time32(`seconds)
  | "ttm" => Time32(`milliseconds)
  | "ttu" => Time64(`microseconds)
  | "ttn" => Time64(`nanoseconds)
  | "tDs" => Duration(`seconds)
  | "tDm" => Duration(`milliseconds)
  | "tDu" => Duration(`microseconds)
  | "tDn" => Duration(`nanoseconds)
  | "tiM" => Interval(`months)
  | "tiD" => Interval(`days_time)
  | "+s" => Struct
  | "+m" => Map
  | unknown =>
    switch (String.split(unknown, ~on=':')) {
    | ["tss", timezone] => Timestamp({precision: `seconds, timezone})
    | ["tsm", timezone] => Timestamp({precision: `milliseconds, timezone})
    | ["tsu", timezone] => Timestamp({precision: `microseconds, timezone})
    | ["tsn", timezone] => Timestamp({precision: `nanoseconds, timezone})
    | ["w", bytes] =>
      switch (Int.of_string(bytes)) {
      | bytes => Fixed_width_binary({bytes: bytes})
      | exception _ => Unknown(unknown)
      }
    | ["d", precision_scale] =>
      switch (String.split(precision_scale, ~on=',')) {
      | [precision, scale] =>
        switch (Int.of_string(precision), Int.of_string(scale)) {
        | (precision, scale) => Decimal128({precision, scale})
        | exception _ => Unknown(unknown)
        }
      | _ => Unknown(unknown)
      }
    | _ => Unknown(unknown)
    };
