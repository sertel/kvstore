{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ExistentialQuantification #-}

module Kvstore.KVSTypes where

import qualified Data.Text.Lazy         as T
import qualified Data.ByteString.Lazy   as BS
import qualified Data.HashMap.Strict    as Map
import           Data.IORef

import qualified DB_Iface               as DB

import           Control.DeepSeq

type Fields = Map.HashMap T.Text T.Text
type Table = Map.HashMap T.Text Fields
type KVStore = Map.HashMap T.Text Table

data Serialization = forall state. NFData state => Serialization
  { _serialize :: state -> Table -> (BS.ByteString, state)
  , _serState :: state
  }

data Deserialization = forall state. NFData state => Deserialization
  { _deserialize :: state -> BS.ByteString -> (Table, state)
  , _deSerState :: state
  }

-- FIXME turn into ADT
class Compression a where
  compress :: a -> BS.ByteString -> BS.ByteString
  decompress :: a -> BS.ByteString -> BS.ByteString

data KVSState a = KVSState {
                  getKvs :: KVStore -- the cache (kv-store)
                , getDbBackend :: DB.DB_Iface a => a -- the storage backend
                , serializer :: Serialization
                , deserializer :: Deserialization
                         -- TODO compression/decompression algo
                         -- TODO (Encryption ) -- the encryption backend
                       }

newtype KVSHandler a = KVSHandler (IORef (KVSState a))
