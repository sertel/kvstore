{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE StandaloneDeriving, DeriveGeneric #-}

module Kvstore.KVSTypes where

import qualified Data.Text.Lazy         as T
import qualified Data.ByteString.Lazy   as BS
import qualified Data.HashMap.Strict    as Map
import           Data.IORef

import qualified DB_Iface               as DB
import Kvservice_Types (KVRequest, Operation, KVResponse)
import GHC.Generics

import           Control.DeepSeq
import           Control.Lens

type Fields = Map.HashMap T.Text T.Text
type Table = Map.HashMap T.Text Fields
type KVStore = Map.HashMap T.Text Table

data Serialization = forall state. NFData state => Serialization
  { _serialize :: state -> Table -> (BS.ByteString, state)
  , _serState :: state
  }

makeLenses ''Serialization

data Deserialization = forall state. NFData state => Deserialization
  { _deserialize :: state -> BS.ByteString -> (Table, state)
  , _deSerState :: state
  }

makeLenses ''Deserialization

data Compression = forall state. NFData state => Compression
  {
    _compress :: state -> BS.ByteString -> (BS.ByteString, state)
  , _compState :: state
}

makeLenses ''Compression

data Decompression = forall state. NFData state => Decompression
  {
    _decompress :: state -> BS.ByteString -> (BS.ByteString, state)
  , _decompState :: state
  }

makeLenses ''Decompression

data Encryption = forall state. NFData state => Encryption
  {
    _encrypt :: state -> BS.ByteString -> (BS.ByteString, state)
  , _encState :: state
  }

makeLenses ''Encryption

data Decryption = forall state. NFData state => Decryption
  {
    _decrypt :: state -> BS.ByteString -> (BS.ByteString, state)
  , _decState :: state
  }

makeLenses ''Decryption

data KVSState a = KVSState
                {
                  _cache :: KVStore
                , _storage :: DB.DB_Iface a => a
                , _serializer :: Serialization
                , _deserializer :: Deserialization
                , _compression :: Compression
                , _decompression :: Decompression
                , _encryption :: Encryption
                , _decryption :: Decryption
                }

makeLenses ''KVSState

newtype KVSHandler a = KVSHandler (IORef (KVSState a))

instance NFData KVRequest
instance NFData Operation
--instance NFData a => NFData (KVSState a)
instance NFData KVResponse

instance NFData Serialization where
  rnf (Serialization _ st) = rnf st

instance NFData Deserialization where
  rnf (Deserialization _ st) = rnf st

instance NFData Encryption where
  rnf (Encryption _ st) = rnf st

instance NFData Decryption where
  rnf (Decryption _ st) = rnf st

instance NFData Compression where
  rnf (Compression _ st) = rnf st

instance NFData Decompression where
  rnf (Decompression _ st) = rnf st
