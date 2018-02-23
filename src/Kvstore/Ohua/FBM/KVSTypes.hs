{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric #-}

module Kvstore.Ohua.FBM.KVSTypes where

import           Kvservice_Types
import           Kvstore.JSONSerialization
import qualified Control.DeepSeq            as DS
import           GHC.Generics

data LocalState serde = Stateless | Serializer serde | Deserializer serde deriving Generic

instance DS.NFData Operation
instance DS.NFData KVResponse

instance DS.NFData JSONSerDe
instance DS.NFData serde => DS.NFData (LocalState serde)
