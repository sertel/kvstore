{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, InstanceSigs #-}

module Kvstore.JSONSerialization where

import           Kvstore.KVSTypes
import qualified Data.Aeson           as AE
import           Data.ByteString.Lazy
import           Data.Maybe
import           GHC.Generics

data JSONSerDe = JSONSerDe

instance SerDe JSONSerDe where
  serialize :: JSONSerDe -> Table -> ByteString
  serialize _ table = AE.encode table

  deserialize :: JSONSerDe -> ByteString -> Table
  deserialize _ = (fromJust.  AE.decode')
