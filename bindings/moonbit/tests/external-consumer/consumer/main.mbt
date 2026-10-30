///|
/// Separate MoonBit module consuming only the public SDK surface.
fn main {
  let db = match @hawdb.open_in_memory() {
    Ok(v) => v
    Err(e) => {
      println("EXTERNAL_CONSUMER_FAIL open: " + e.describe())
      return
    }
  }
  match db.query_sql("CREATE TABLE sdk_consumer (id BIGINT PRIMARY KEY, amount BIGINT)", []) {
    Ok(_) => ()
    Err(e) => {
      println("EXTERNAL_CONSUMER_FAIL schema: " + e.describe())
      db.close()
      return
    }
  }
  let inserted : Result[String, @hawdb.HawdbError] = db.with_transaction(fn(tx) {
    match tx.query_sql(
      "INSERT INTO sdk_consumer (id, amount) VALUES ($1, $2)",
      [@hawdb.Value::Int(42L), @hawdb.Value::Int(99L)],
    ) {
      Ok(_) => Ok("committed")
      Err(e) => Err(e)
    }
  })
  match inserted {
    Ok("committed") => ()
    Err(e) => {
      println("EXTERNAL_CONSUMER_FAIL transaction: " + e.describe())
      db.close()
      return
    }
    _ => {
      println("EXTERNAL_CONSUMER_FAIL transaction result")
      db.close()
      return
    }
  }
  let result = db.query_sql("SELECT id, amount FROM sdk_consumer ORDER BY id", [])
  let matched = match result {
    Ok(rows) =>
      match (rows.get(0, "id"), rows.get(0, "amount"), rows.get(0, "missing")) {
        (Some(@hawdb.Value::Int(42L)), Some(@hawdb.Value::Int(99L)), None) => true
        _ => false
      }
    Err(_) => false
  }
  db.close()
  if matched {
    println("EXTERNAL_CONSUMER_PASS")
  } else {
    println("EXTERNAL_CONSUMER_FAIL result")
  }
}
