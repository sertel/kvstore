
module Kvstore.Ohua.FBM.KVSTypes where

data LocalState serde = Stateless | Serializer serde | Deserializer serde
