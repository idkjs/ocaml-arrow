open Base;
open Ppxlib;
open Ast_builder.Default;

let raise_errorf = (~loc, fmt) =>
  Location.raise_errorf(~loc, Caml.(^^)("ppx_arrow: ", fmt));

let floatable =
  Attribute.declare(
    "arrow.floatable",
    Attribute.Context.label_declaration,
    Ast_pattern.(pstr(nil)),
    x =>
    x
  );

let intable =
  Attribute.declare(
    "arrow.intable",
    Attribute.Context.label_declaration,
    Ast_pattern.(pstr(nil)),
    x =>
    x
  );

let stringable =
  Attribute.declare(
    "arrow.stringable",
    Attribute.Context.label_declaration,
    Ast_pattern.(pstr(nil)),
    x =>
    x
  );

let sexpable =
  Attribute.declare(
    "arrow.sexpable",
    Attribute.Context.label_declaration,
    Ast_pattern.(pstr(nil)),
    x =>
    x
  );

let boolable =
  Attribute.declare(
    "arrow.boolable",
    Attribute.Context.label_declaration,
    Ast_pattern.(pstr(nil)),
    x =>
    x
  );

module Attr = {
  type t = {
    kind: [ | `intable | `stringable | `sexpable | `floatable | `boolable],
    is_option: bool,
  };
};

let attribute = (field, ~loc) => {
  let intable = Attribute.get(intable, field);
  let floatable = Attribute.get(floatable, field);
  let stringable = Attribute.get(stringable, field);
  let sexpable = Attribute.get(sexpable, field);
  let boolable = Attribute.get(boolable, field);
  let is_option =
    switch (field.pld_type.ptyp_desc) {
    | [@implicit_arity] Ptyp_constr({txt: Lident("option"), _}, [_]) =>
      true
    | _ => false
    };

  switch (intable, floatable, stringable, sexpable, boolable) {
  | (Some(_), None, None, None, None) =>
    Some({Attr.kind: `intable, is_option})
  | (None, Some(_), None, None, None) =>
    Some({Attr.kind: `floatable, is_option})
  | (None, None, Some(_), None, None) =>
    Some({Attr.kind: `stringable, is_option})
  | (None, None, None, Some(_), None) =>
    Some({Attr.kind: `sexpable, is_option})
  | (None, None, None, None, Some(_)) =>
    Some({Attr.kind: `boolable, is_option})
  | (None, None, None, None, None) => None
  | _ =>
    raise_errorf(
      "cannot have more than one of intable, floatable, boolable, sexpable, or stringable",
      ~loc,
    )
  };
};

let lident = (~loc, str) => Loc.make(~loc, Lident(str));

let fresh_label = {
  let counter = ref(0);
  (~loc) => {
    Int.incr(counter);
    let label = Printf.sprintf("_lbl_%d", counter^);
    (
      ppat_var(Loc.make(~loc, label), ~loc),
      pexp_ident(lident(~loc, label), ~loc),
    );
  };
};

let closure_of_fn = (fn: expression => expression, ~loc): expression => {
  let loc = {...loc, loc_ghost: true};
  let (arg_pat, arg_expr) = fresh_label(~loc);
  pexp_fun(Nolabel, ~loc, None, arg_pat, fn(arg_expr));
};

let open_runtime = (expr, ~loc) => [%expr
  {
    open! Ppx_arrow_runtime;
    %e
    expr;
  }
];

/* If a field has type [A.B.t] return [A.B.fn_name] as an expr. */
let fn_from_field_module = (field, ~fn_name, ~loc) => {
  let ident =
    switch (field.pld_type.ptyp_desc) {
    | [@implicit_arity]
      Ptyp_constr(
        {txt: Lident("option"), _},
        [{ptyp_desc: [@implicit_arity] Ptyp_constr({txt, _}, []), _}],
      )
    | [@implicit_arity] Ptyp_constr({loc: _, txt}, []) =>
      switch (txt) {
      | Lident(_) => lident(~loc, fn_name)
      | [@implicit_arity] Ldot(modl, _) =>
        [@implicit_arity] Ldot(modl, fn_name) |> Loc.make(~loc)
      | Lapply(_) =>
        raise_errorf(~loc, "'%s' apply not supported", Longident.name(txt))
      }
    | _ =>
      raise_errorf(
        ~loc,
        "cannot extract module name from '%a'",
        Pprintast.core_type,
        field.pld_type,
      )
    };

  pexp_ident(ident, ~loc);
};

/* Generated function names. */
let arrow_read = tname => "arrow_read_" ++ tname;
let arrow_write = tname => "arrow_write_" ++ tname;
let arrow_of_table = tname => "arrow_" ++ tname ++ "_of_table";
let arrow_table_of = tname => "arrow_table_of_" ++ tname;

module Signature: {
  let gen:
    [ | `read | `write | `both] =>
    Deriving.Generator.t(signature, (rec_flag, list(type_declaration)));
} = {
  let read_type = (td, ~loc) => [%type:
    string => array([%t Ppxlib.core_type_of_type_declaration(td)])
  ];

  let write_type = (td, ~loc) => [%type:
    (array([%t Ppxlib.core_type_of_type_declaration(td)]), string) => unit
  ];

  let table_of_type = (td, ~loc) => [%type:
    array([%t Ppxlib.core_type_of_type_declaration(td)]) =>
    Arrow_c_api.Table.t
  ];

  let of_table_type = (td, ~loc) => [%type:
    Arrow_c_api.Table.t =>
    array([%t Ppxlib.core_type_of_type_declaration(td)])
  ];

  let of_td = (~kind, td): list(signature_item) => {
    let {Location.loc, txt: tname} = td.ptype_name;
    if (!List.is_empty(td.ptype_params)) {
      raise_errorf("parametered types are not supported", ~loc);
    };
    let psig_value = (~name, ~type_) =>
      psig_value(
        ~loc,
        value_description(
          ~loc,
          ~name=Loc.make(name, ~loc),
          ~type_,
          ~prim=[],
        ),
      );

    let read_type = read_type(td, ~loc);
    let write_type = write_type(td, ~loc);
    let table_of_type = table_of_type(td, ~loc);
    let of_table_type = of_table_type(td, ~loc);
    switch (kind) {
    | `both => [
        psig_value(~name=arrow_read(tname), ~type_=read_type),
        psig_value(~name=arrow_write(tname), ~type_=write_type),
        psig_value(~name=arrow_of_table(tname), ~type_=of_table_type),
        psig_value(~name=arrow_table_of(tname), ~type_=table_of_type),
      ]
    | `read => [
        psig_value(~name=arrow_read(tname), ~type_=read_type),
        psig_value(~name=arrow_of_table(tname), ~type_=of_table_type),
      ]
    | `write => [
        psig_value(~name=arrow_write(tname), ~type_=write_type),
        psig_value(~name=arrow_table_of(tname), ~type_=table_of_type),
      ]
    };
  };

  let gen = kind =>
    Deriving.Generator.make_noarg((~loc as _, ~path as _, (_rec_flag, tds)) =>
      List.concat_map(tds, ~f=of_td(~kind))
    );
};

module Structure: {
  let gen:
    [ | `read | `write | `both] =>
    Deriving.Generator.t(structure, (rec_flag, list(type_declaration)));
} = {
  let expr_of_tds = (~loc, ~record, tds) => {
    let exprs =
      List.map(
        tds,
        ~f=td => {
          let {Location.loc, txt: _} = td.ptype_name;
          if (!List.is_empty(td.ptype_params)) {
            raise_errorf("parametered types are not supported", ~loc);
          };
          let expr = arg_t =>
            switch (td.ptype_kind) {
            | Ptype_abstract =>
              raise_errorf(~loc, "abstract types not supported")
            | Ptype_variant(_) =>
              raise_errorf(~loc, "variant types not supported")
            | Ptype_record(fields) => record(fields, ~loc, arg_t)
            | Ptype_open => raise_errorf(~loc, "open types not supported")
            };

          closure_of_fn(expr, ~loc);
        },
      );

    pexp_tuple(~loc, exprs);
  };

  let extract_ident = (ident, ~loc) => {
    let rec loop =
      fun
      | Lident(ident) => [ident]
      | [@implicit_arity] Ldot(modl, ident) => [ident, ...loop(modl)]
      | Lapply(_) =>
        raise_errorf(
          ~loc,
          "'%s' apply not supported",
          Longident.name(ident),
        );

    loop(ident);
  };

  let runtime_fn = (field, ~fn_name, ~loc) => {
    let modl =
      switch (attribute(field, ~loc)) {
      | Some({kind: `intable, is_option: false}) => "Int_col"
      | Some({kind: `intable, is_option: true}) => "Int_option_col"
      | Some({kind: `floatable, is_option: false}) => "Float_col"
      | Some({kind: `floatable, is_option: true}) => "Float_option_col"
      | Some({kind: `stringable | `sexpable, is_option: false}) => "String_col"
      | Some({kind: `stringable | `sexpable, is_option: true}) => "String_option_col"
      | Some({kind: `boolable, is_option: false}) => "Bool_col"
      | Some({kind: `boolable, is_option: true}) => "Bool_option_col"
      | None =>
        switch (field.pld_type.ptyp_desc) {
        | [@implicit_arity] Ptyp_constr({loc: _, txt}, []) =>
          switch (extract_ident(txt, ~loc)) {
          | [] => assert(false)
          | [ident] => String.capitalize(ident) ++ "_col"
          | ["t", modl, ..._] => modl ++ "_col"
          | _ =>
            raise_errorf(
              ~loc,
              "'%s' base type not supported",
              Longident.name(txt),
            )
          }
        | [@implicit_arity]
          Ptyp_constr(
            {txt: Lident("option"), _},
            [{ptyp_desc: [@implicit_arity] Ptyp_constr({txt, _}, []), _}],
          ) =>
          switch (extract_ident(txt, ~loc)) {
          | [] => assert(false)
          | [ident] => String.capitalize(ident) ++ "_option_col"
          | ["t", modl, ..._] => modl ++ "_option_col"
          | _ =>
            raise_errorf(
              ~loc,
              "'%s' option type not supported",
              Longident.name(txt),
            )
          }
        | _ =>
          raise_errorf(
            ~loc,
            "'%a' not supported",
            Pprintast.core_type,
            field.pld_type,
          )
        }
      };

    pexp_ident(
      Loc.make([@implicit_arity] Ldot(Lident(modl), fn_name), ~loc),
      ~loc,
    );
  };

  let read_or_of_table = (fields, ~loc, args, ~which) => {
    let record_fields =
      List.map(
        fields,
        ~f=field => {
          let get = runtime_fn(field, ~fn_name="get", ~loc);
          let expr = [%expr
            [%e get]([%e evar(field.pld_name.txt, ~loc)], idx)
          ];
          let apply_fn = (~fn_name, ~is_option) => {
            let fn = fn_from_field_module(field, ~fn_name, ~loc);
            if (is_option) {
              %expr
              Option.map(~f=[%e fn], [%e expr]);
            } else {
              %expr
              [%e fn]([%e expr]);
            };
          };

          let expr =
            switch (attribute(field, ~loc)) {
            | Some({kind: `boolable, is_option}) =>
              apply_fn(~fn_name="of_bool", ~is_option)
            | Some({kind: `stringable, is_option}) =>
              apply_fn(~fn_name="of_string", ~is_option)
            | Some({kind: `sexpable, is_option}) =>
              let typ_ = field.pld_type;
              if (is_option) {
                %expr
                Sexp.of_string([%e expr])
                |> Option.map(~f=[%of_sexp: [%t typ_]]);
              } else {
                %expr
                Sexp.of_string([%e expr]) |> [%of_sexp: [%t typ_]];
              };
            | Some({kind: `floatable, is_option}) =>
              apply_fn(~fn_name="of_float", ~is_option)
            | Some({kind: `intable, is_option}) =>
              apply_fn(~fn_name="of_int_exn", ~is_option)
            | None => expr
            };

          (lident(field.pld_name.txt, ~loc), expr);
        },
      );

    let pat = str => ppat_var(Loc.make(~loc, str), ~loc);
    let create_columns =
      List.map(
        fields,
        ~f=field => {
          let name_as_string = estring(~loc, field.pld_name.txt);
          let of_table = runtime_fn(field, ~fn_name="of_table", ~loc);
          let expr = [%expr [%e of_table](table, [%e name_as_string])];
          value_binding(~loc, ~pat=pat(field.pld_name.txt), ~expr);
        },
      );

    let input_table_expr =
      switch (which) {
      | `read =>
        let col_names =
          List.map(fields, ~f=field => estring(~loc, field.pld_name.txt));

        %expr
        Arrow_c_api.File_reader.table(
          ~columns=`names([%e elist(col_names, ~loc)]),
          [%e args],
        );
      | `of_table => args
      };

    [%expr
      Caml.Array.init(Arrow_c_api.Table.num_rows(table), idx =>{
        %e
        pexp_record(record_fields, ~loc, None)
      })
    ]
    |> pexp_let(~loc, Nonrecursive, create_columns)
    |> pexp_let(
         ~loc,
         Nonrecursive,
         [value_binding(~loc, ~pat=pat("table"), ~expr=input_table_expr)],
       )
    |> open_runtime(~loc);
  };

  let read_fields = read_or_of_table(~which=`read);
  let of_table_fields = read_or_of_table(~which=`of_table);

  let write_or_table_of = (fields, ~loc, args, ~which) => {
    let pat = str => ppat_var(Loc.make(~loc, str), ~loc);
    let create_columns =
      List.map(
        fields,
        ~f=field => {
          let init = runtime_fn(field, ~fn_name="init", ~loc);
          let expr = [%expr [%e init](__arrow_len)];
          value_binding(~loc, ~pat=pat(field.pld_name.txt), ~expr);
        },
      );

    let set_columns =
      List.map(
        fields,
        ~f=field => {
          let set = runtime_fn(field, ~fn_name="set", ~loc);
          let array = evar(field.pld_name.txt, ~loc);
          let value =
            pexp_field(
              [%expr [%e args][__arrow_idx]],
              lident(~loc, field.pld_name.txt),
              ~loc,
            );

          let apply_fn = (~fn_name, ~is_option) => {
            let fn = fn_from_field_module(field, ~fn_name, ~loc);
            if (is_option) {
              %expr
              Option.map(~f=[%e fn], [%e value]);
            } else {
              %expr
              [%e fn]([%e value]);
            };
          };

          let value =
            switch (attribute(field, ~loc)) {
            | Some({kind: `boolable, is_option}) =>
              apply_fn(~fn_name="to_bool", ~is_option)
            | Some({kind: `stringable, is_option}) =>
              apply_fn(~fn_name="to_string", ~is_option)
            | Some({kind: `sexpable, is_option}) =>
              let typ_ = field.pld_type;
              if (is_option) {
                %expr
                Option.map(~f=[%sexp_of: [%t typ_]], [%e value])
                |> Sexp.to_string;
              } else {
                %expr
                [%sexp_of: [%t typ_]]([%e value]) |> Sexp.to_string;
              };
            | Some({kind: `floatable, is_option}) =>
              apply_fn(~fn_name="to_float", ~is_option)
            | Some({kind: `intable, is_option}) =>
              apply_fn(~fn_name="to_int_exn", ~is_option)
            | None => value
            };

          %expr
          [%e set]([%e array], __arrow_idx, [%e value]);
        },
      );

    let col_list =
      List.map(
        fields,
        ~f=field => {
          let name_as_string = estring(~loc, field.pld_name.txt);
          let writer_col = runtime_fn(field, ~fn_name="writer_col", ~loc);
          let array = evar(field.pld_name.txt, ~loc);
          %expr
          [%e writer_col]([%e array], [%e name_as_string]);
        },
      );

    switch (which) {
    | `table_of =>
      [%expr
        {
          for (__arrow_idx in 0 to __arrow_len - 1) {
            %e
            esequence(set_columns, ~loc);
          };
          Arrow_c_api.Writer.create_table(~cols=[%e elist(col_list, ~loc)]);
        }
      ]
      |> pexp_let(~loc, Nonrecursive, create_columns)
      |> pexp_let(
           ~loc,
           Nonrecursive,
           [
             value_binding(
               ~loc,
               ~pat=pat("__arrow_len"),
               ~expr=[%expr Caml.Array.length([%e args])],
             ),
           ],
         )
      |> open_runtime(~loc)
    | `write =>
      closure_of_fn(~loc, filename =>
        [%expr
          {
            for (__arrow_idx in 0 to __arrow_len - 1) {
              %e
              esequence(set_columns, ~loc);
            };
            Arrow_c_api.Writer.write(
              [%e filename],
              ~cols=[%e elist(col_list, ~loc)],
            );
          }
        ]
        |> pexp_let(~loc, Nonrecursive, create_columns)
        |> pexp_let(
             ~loc,
             Nonrecursive,
             [
               value_binding(
                 ~loc,
                 ~pat=pat("__arrow_len"),
                 ~expr=[%expr Caml.Array.length([%e args])],
               ),
             ],
           )
        |> open_runtime(~loc)
      )
    };
  };

  let write_fields = write_or_table_of(~which=`write);
  let table_of_fields = write_or_table_of(~which=`table_of);

  let gen = kind => {
    let attributes = [
      Attribute.T(intable),
      Attribute.T(floatable),
      Attribute.T(stringable),
      Attribute.T(boolable),
    ];

    Deriving.Generator.make_noarg(
      ~attributes,
      (~loc, ~path as _, (rec_flag, tds)) => {
        let mk_pat = mk_ => {
          let pats =
            List.map(
              tds,
              ~f=td => {
                let {Location.loc, txt: tname} = td.ptype_name;
                let name = mk_(tname);
                ppat_var(~loc, Loc.make(name, ~loc));
              },
            );

          ppat_tuple(~loc, pats);
        };

        let tds = List.map(tds, ~f=name_type_params_in_td);
        let read_expr = expr_of_tds(~loc, ~record=read_fields);
        let write_expr = expr_of_tds(~loc, ~record=write_fields);
        let table_of_expr = expr_of_tds(~loc, ~record=table_of_fields);
        let of_table_expr = expr_of_tds(~loc, ~record=of_table_fields);
        let bindings =
          switch (kind) {
          | `both => [
              value_binding(
                ~loc,
                ~pat=mk_pat(arrow_read),
                ~expr=read_expr(tds),
              ),
              value_binding(
                ~loc,
                ~pat=mk_pat(arrow_write),
                ~expr=write_expr(tds),
              ),
              value_binding(
                ~loc,
                ~pat=mk_pat(arrow_of_table),
                ~expr=of_table_expr(tds),
              ),
              value_binding(
                ~loc,
                ~pat=mk_pat(arrow_table_of),
                ~expr=table_of_expr(tds),
              ),
            ]
          | `read => [
              value_binding(
                ~loc,
                ~pat=mk_pat(arrow_read),
                ~expr=read_expr(tds),
              ),
              value_binding(
                ~loc,
                ~pat=mk_pat(arrow_of_table),
                ~expr=of_table_expr(tds),
              ),
            ]
          | `write => [
              value_binding(
                ~loc,
                ~pat=mk_pat(arrow_write),
                ~expr=write_expr(tds),
              ),
              value_binding(
                ~loc,
                ~pat=mk_pat(arrow_table_of),
                ~expr=table_of_expr(tds),
              ),
            ]
          };

        [pstr_value(~loc, really_recursive(rec_flag, tds), bindings)];
      },
    );
  };
};

let arrow =
  Deriving.add(
    "arrow",
    ~str_type_decl=Structure.gen(`both),
    ~sig_type_decl=Signature.gen(`both),
  );

module Reader = {
  let name = "arrow_reader";

  let deriver =
    Deriving.add(
      name,
      ~str_type_decl=Structure.gen(`read),
      ~sig_type_decl=Signature.gen(`read),
    );
};

module Writer = {
  let name = "arrow_writer";

  let deriver =
    Deriving.add(
      name,
      ~str_type_decl=Structure.gen(`write),
      ~sig_type_decl=Signature.gen(`write),
    );
};
