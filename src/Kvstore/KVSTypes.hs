{-# LANGUAGE RankNTypes #-}

module Kvstore.KVSTypes where

import qualified Data.Text.Lazy         as T
import qualified Data.HashMap.Strict    as Map
import           Data.IORef

import qualified DB_Iface               as DB


type Fields = Map.HashMap T.Text T.Text
type Table = Map.HashMap T.Text Fields
type KVStore = Map.HashMap T.Text Table

class SerDe a where
  serialize :: a -> Table -> T.Text
  deserialize :: a -> T.Text -> Table

class Compression a where
  compress :: a -> T.Text -> T.Text
  decompress :: a -> T.Text -> T.Text

data KVSState a b = KVSState {
                  getKvs :: KVStore -- the cache (kv-store)
                , getDbBackend :: (DB.DB_Iface a => a) -- the storage backend
                , getSerDe :: (SerDe b => b) -- serialization/deserialization
                         -- TODO compression/decompression algo
                         -- TODO (Encryption ) -- the encryption backend
                       }

data KVSHandler a b = KVSHandler (IORef (KVSState a b))
