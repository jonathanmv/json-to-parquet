namespace java jonathanmv.json_to_parquet.model

union PersonID {
  1: string person_id;
}

struct FriendsEdge {
  1: required PersonID one;
  2: required PersonID two;
  3: required i64 timestamp;
}