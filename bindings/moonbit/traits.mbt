///|
/// Extension point for application records encoded as Cypher named parameters.
pub(open) trait HawdbParams {
  to_params(Self) -> Map[String, Value]
}

///|
/// Extension point for named-column decoding of a materialized result.
/// Missing columns and type mismatches are errors, not null/default values.
pub(open) trait HawdbRow {
  from_row(QueryResult, Int) -> Result[Self, HawdbError]
}

///|
pub fn[T : HawdbRow] from_row(
  result : QueryResult,
  index : Int,
) -> Result[T, HawdbError] {
  T::from_row(result, index)
}

///|
pub fn[T : HawdbParams] to_params(value : T) -> Map[String, Value] {
  value.to_params()
}
