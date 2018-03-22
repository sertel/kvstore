
struct DBResponse {
 1: optional binary value
}

service DB{
  DBResponse get(1:string key),
  void put(1:string key, 2:binary value)
}
