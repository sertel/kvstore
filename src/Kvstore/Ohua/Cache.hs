
module Kvstore.Ohua.Cache where

import           Kvservice_Types

import           Control.Monad.State
import           Control.Lens
import qualified Data.Text.Lazy          as T
import qualified Data.ByteString.Lazy    as BS
import qualified Data.Vector             as Vector
import qualified Data.HashMap.Strict     as Map
import qualified Data.Set                as Set
import           Data.Maybe

import qualified DB_Iface                as DB
import           Kvstore.KVSTypes
import           Kvstore.InputOutput

import           Debug.Trace

import           Kvstore.Ohua.KVSTypes

loadTableSF :: (DB.DB_Iface a) => a -> T.Text -> StateT Stateless IO (Maybe BS.ByteString)
loadTableSF db tableId = liftIO $ evalStateT (loadTable tableId) KVSState{ _storage=db }

deserializeTableSF :: BS.ByteString -> StateT Deserialization IO Table
deserializeTableSF d = do
  deser <- get
  (r, kvsstate) <- liftIO $ runStateT (deserializeTable d) KVSState{ _deserializer=deser }
  put $ view deserializer kvsstate
  return r

decryptTableSF :: BS.ByteString -> StateT Decryption IO BS.ByteString
decryptTableSF d = do
  decrypter <- get
  (r, KVSState{ _decryption=decrypter' }) <- liftIO $ runStateT (decryptTable d) KVSState{ _decryption=decrypter }
  put decrypter'
  return r

decompressTableSF :: BS.ByteString -> StateT Decompression IO BS.ByteString
decompressTableSF d = do
  decomp <- get
  (r, KVSState{ _decompression=decomp' }) <- liftIO $ runStateT (decompressTable d) KVSState{ _decompression=decomp }
  put decomp'
  return r

encryptTableSF :: BS.ByteString -> StateT Encryption IO BS.ByteString
encryptTableSF d = do
  encrypter <- get
  (r, KVSState{ _encryption=encrypter' }) <- liftIO $ runStateT (encryptTable d) KVSState{ _encryption=encrypter }
  put encrypter'
  return r

compressTableSF :: BS.ByteString -> StateT Compression IO BS.ByteString
compressTableSF d = do
  comp <- get
  (r, KVSState{ _compression=comp' }) <- liftIO $ runStateT (compressTable d) KVSState{ _compression=comp }
  put comp'
  return r
