
module Kvstore.Ohua.Cache where

import           Kvservice_Types

import           Control.Monad.State
import qualified Data.Text.Lazy          as T
import qualified Data.ByteString.Lazy    as BS
import qualified Data.Vector             as Vector
import qualified Data.HashMap.Strict     as Map
import qualified Data.Set                as Set
import           Data.Maybe
import Control.DeepSeq

import qualified DB_Iface                as DB
import           Kvstore.KVSTypes
import           Kvstore.InputOutput
import           Kvstore.Ohua.KVSTypes

import           Debug.Trace


useStrictness :: Bool
useStrictness = True

forceA :: (Applicative f, NFData a) => a -> f a
forceA a = a `deepseq` pure a

withForceA :: (Monad m, NFData a) => m a -> m a
withForceA = (forceA =<<)

withStrictness :: (Monad m, NFData a) => m a -> m a
withStrictness | useStrictness = withForceA
               | otherwise = id

forceLazyByteString :: BS.ByteString -> ()
forceLazyByteString = BS.foldrChunks deepseq ()

withForceByteString :: Monad m => m BS.ByteString -> m BS.ByteString
withForceByteString bs
    | useStrictness = bs >>= \bs' -> forceLazyByteString bs' `deepseq` pure bs'
    | otherwise = bs

loadTableSF :: (DB.DB_Iface a) => a -> T.Text -> StateT Stateless IO (Maybe BS.ByteString)
loadTableSF db tableId = liftIO $ evalStateT (loadTable tableId >>= \bs -> maybe () forceLazyByteString bs `deepseq` pure bs) KVSState{ _storage=db }

deserializeTableSF :: BS.ByteString -> StateT Deserialization IO Table
deserializeTableSF d = do
  deser <- get
  (r, KVSState{ _deserializer=deser' }) <- liftIO $ runStateT (deserializeTable d) KVSState{ _deserializer=deser }
  put deser'
  return r

decryptTableSF :: BS.ByteString -> StateT Decryption IO BS.ByteString
decryptTableSF d = do
  decrypter <- get
  (r, KVSState{ _decryption=decrypter' }) <- liftIO $ runStateT (withForceByteString $ decryptTable d) KVSState{ _decryption=decrypter }
  put decrypter'
  return r

decompressTableSF :: BS.ByteString -> StateT Decompression IO BS.ByteString
decompressTableSF d = do
  decomp <- get
  (r, KVSState{ _decompression=decomp' }) <- liftIO $ runStateT (withForceByteString $ decompressTable d) KVSState{ _decompression=decomp }
  put decomp'
  return r

encryptTableSF :: BS.ByteString -> StateT Encryption IO BS.ByteString
encryptTableSF d = do
  encrypter <- get
  (r, KVSState{ _encryption=encrypter' }) <- liftIO $ runStateT (withForceByteString $ encryptTable d) KVSState{ _encryption=encrypter }
  put encrypter'
  return r

compressTableSF :: BS.ByteString -> StateT Compression IO BS.ByteString
compressTableSF d = do
  comp <- get
  (r, KVSState{ _compression=comp' }) <- liftIO $ runStateT (withForceByteString $ compressTable d) KVSState{ _compression=comp }
  put comp'
  return r

serializeTableSF :: Table -> StateT Serialization IO BS.ByteString
serializeTableSF table = do
    ser <- get
    (r, KVSState {_serializer = ser'}) <-
        liftIO $
        runStateT (withForceByteString $ serializeTable table) KVSState {_serializer = ser}
    put ser'
    return r
