{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, InstanceSigs #-}
{-# LANGUAGE DeriveGeneric #-}

module Kvstore.JSONSerialization where

import           Kvstore.KVSTypes
import qualified Data.Aeson           as AE
import           Data.ByteString.Lazy
import           Data.Maybe
import           GHC.Generics

data JSONSerDe = JSONSerDe deriving Generic

instance SerDe JSONSerDe where
  serialize :: JSONSerDe -> Table -> ByteString
  serialize _ = AE.encode

  deserialize :: JSONSerDe -> ByteString -> Table
  deserialize _ = fromJust . AE.decode'
