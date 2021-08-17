let schema: string => Wrapper.Schema.t;
let table:
  (~columns: [ | `indexes(list(int)) | `names(list(string))]=?, string) =>
  Table.t;
