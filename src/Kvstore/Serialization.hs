{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, InstanceSigs #-}

module Kvstore.Serialization where

import           Kvstore.KVSTypes
import qualified Data.Aeson           as AE
import           Data.ByteString.Lazy
import           Data.Maybe
import           GHC.Generics
import           Control.DeepSeq

serialize :: Serialization -> Table -> (Serialization, ByteString)
serialize (Serialization ser s) tbl = let (bs, s') = ser s tbl in (Serialization ser s', bs)

deserialize :: Deserialization -> ByteString -> (Deserialization, Table)
deserialize (Deserialization de s) bs = let (tbl, s') = de s bs in (Deserialization de s', tbl)

jsonSer :: Serialization
jsonSer = Serialization
  { _serialize = \() tbl -> (AE.encode tbl, ())
  , _serState = ()
  }

jsonDeSer :: Deserialization
jsonDeSer = Deserialization
  { _deserialize = \() bs -> ((fromJust . AE.decode) bs, ())
  , _deSerState = ()
  }
