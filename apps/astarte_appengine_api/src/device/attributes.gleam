import appendex
import gleam/list
import gleam/option.{type Option}
import gleam/result

pub type Attributes {
  Attributes(to_delete: List(String), to_update: List(#(String, String)))
}

pub type Errors {
  InvalidAttributes
}

pub type Input =
  List(#(String, Option(String)))

pub fn validate(
  attribute_changes: List(#(String, Option(String))),
) -> Result(Attributes, Errors) {
  use _ <- result.try(validate_format(attribute_changes))

  let #(to_delete, to_update) =
    list.partition(attribute_changes, fn(attribute) {
      let #(_key, value) = attribute
      option.is_none(value)
    })
  let to_delete = to_delete |> list.map(fn(attribute) { attribute.0 })
  let to_update =
    to_update
    |> list.map(fn(attribute) {
      let #(key, value) = attribute
      let assert option.Some(value) = value
      #(key, value)
    })

  let attributes = Attributes(to_delete, to_update)
  Ok(attributes)
}

fn validate_format(attribute_changes: Input) -> Result(Nil, Errors) {
  let invalid_attribute = attribute_changes |> list.key_find(find: "")

  case invalid_attribute {
    Ok(_) -> {
      appendex.warning("Attribute key cannot be an empty string.", [
        #("tag", "invalid_attribute_empty_key"),
      ])
      Error(InvalidAttributes)
    }
    Error(Nil) -> Ok(Nil)
  }
}
