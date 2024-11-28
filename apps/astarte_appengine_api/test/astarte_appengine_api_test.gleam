import device
import gleeunit
import gleeunit/should

pub fn main() {
  gleeunit.main()
}

pub fn gleam_code_test() {
  device.sum(2, 1)
  |> should.equal(4)
}
