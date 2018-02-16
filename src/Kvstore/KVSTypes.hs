{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DeriveGeneric #-}

module Kvstore.KVSTypes where

import qualified Data.Text.Lazy         as T
import qualified Data.ByteString.Lazy   as BS
import qualified Data.HashMap.Strict    as Map
import           Data.IORef

import qualified DB_Iface               as DB

import           FuturesBasedMonad

type Fields = Map.HashMap T.Text T.Text
type Table = Map.HashMap T.Text Fields
type KVStore = Map.HashMap T.Text Table

class SerDe a where
  serialize :: a -> Table -> BS.ByteString
  deserialize :: a -> BS.ByteString -> Table

class Compression a where
  compress :: a -> BS.ByteString -> BS.ByteString
  decompress :: a -> BS.ByteString -> BS.ByteString

data KVSState a b = KVSState {
                  getKvs :: KVStore -- the cache (kv-store)
                , getDbBackend :: DB.DB_Iface a => a -- the storage backend
                , getSerDe :: SerDe b => b -- serialization/deserialization
                         -- TODO compression/decompression algo
                         -- TODO (Encryption ) -- the encryption backend
                       } --deriving (Generic)

newtype KVSHandler a b = KVSHandler (IORef (KVSState a b))
