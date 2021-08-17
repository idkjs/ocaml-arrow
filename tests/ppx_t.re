open! Core_kernel;

[@deriving (arrow, sexp_of)]
type t = {
  x: int,
  y: float,
  z: string,
  x_opt: option(int),
  y_opt: option(float),
  z_opt: option(string),
};
