
module Kvstore.Ohua.FBM.KVSTypes where

import           Kvservice_Types
import qualified Control.DeepSeq    as DS

data LocalState serde = Stateless | Serializer serde | Deserializer serde

instance DS.NFData Operation
instance DS.NFData KVResponse
