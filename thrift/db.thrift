
struct DBResponse {
 1: optional string value
}

service DB{
  DBResponse get(1:string key),
  void put(1:string key, 2:string value)
}
