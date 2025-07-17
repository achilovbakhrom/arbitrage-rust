# check performance
cargo clippy -- \
  -W clippy::perf \
  -W clippy::large_enum_variant \
  -W clippy::inefficient_to_string \
  -W clippy::clone_on_ref_ptr \
  -W clippy::redundant_allocation \
  -W clippy::vec_box \
  -W clippy::box_collection \
  -W clippy::rc_buffer

# apply env args command
export $(grep -v '^#' .env | xargs)