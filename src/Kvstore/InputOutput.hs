{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ExplicitForAll #-}
{-# LANGUAGE RankNTypes #-}

module Kvstore.InputOutput where

import           Control.Monad.State
import           Control.Lens
import qualified Data.Text.Lazy          as T
import qualified Data.ByteString.Lazy    as BS
import qualified Data.Text.Lazy.Encoding as Enc

import qualified DB_Iface                as DB
import           Db_Types
import           Kvstore.KVSTypes

import           Debug.Trace


loadTable :: DB.DB_Iface a => T.Text -> StateT (KVSState a) IO (Maybe BS.ByteString)
loadTable tableId = do
  kvsstate <- get
  let db = view storage kvsstate
  serializedValTable <- liftIO $ DB.get db tableId
  case serializedValTable of
    (DBResponse Nothing) -> return Nothing
    (DBResponse (Just v)) -> return $ Just $ Enc.encodeUtf8 v

deserializeTable :: BS.ByteString -> StateT (KVSState a) IO Table
deserializeTable serializedTable = do
  (t, kvsstate') <- compute_ <$> get
  put kvsstate'
  return t
    where
      compute_ kvs =
        case view deserializer kvs of
          (Deserialization deser s) ->
                  let (t, s') = deser s serializedTable
                  in  ((t,) . over deserializer (const $ Deserialization deser s')) kvs

storeTable :: DB.DB_Iface a => T.Text -> BS.ByteString -> StateT (KVSState a) IO ()
storeTable key serializedTable = do
  db <- view storage <$> get
  _ <- liftIO $ DB.put db key $ Enc.decodeUtf8 serializedTable
  return ()

serializeTable :: Table -> StateT (KVSState a) IO BS.ByteString
serializeTable table = do
  (t, kvsstate') <- compute_ <$> get
  put kvsstate'
  return t
    where
      compute_ kvs = case view serializer kvs of
                        (Serialization ser s) ->
                              let (t, s') = ser s table
                              in ((t,) . over serializer (const $ Serialization ser s')) kvs

encryptTable :: BS.ByteString -> StateT (KVSState a) IO BS.ByteString
encryptTable table = do
  (t, kvsstate') <- compute_ <$> get
  put kvsstate'
  return t
    where
      compute_ kvsstate@KVSState{_encryption=(Encryption enc s)} =
                              let (t, s') = enc s table
                              in (t,kvsstate{_encryption=Encryption enc s'})

decryptTable :: BS.ByteString -> StateT (KVSState a) IO BS.ByteString
decryptTable table = do
  (t, kvsstate') <- compute_ <$> get
  put kvsstate'
  return t
    where
      compute_ kvsstate@KVSState{_decryption=(Decryption dec s)} =
                              let (t, s') = dec s table
                              in (t,kvsstate{_decryption=Decryption dec s'})


compressTable :: BS.ByteString -> StateT (KVSState a) IO BS.ByteString
compressTable table = do
  (t, kvsstate') <- compute_ <$> get
  put kvsstate'
  return t
    where
      compute_ kvsstate@KVSState{_compression=(Compression comp s)} =
                              let (t, s') = comp s table
                              in (t,kvsstate{_compression=Compression comp s'})


decompressTable :: BS.ByteString -> StateT (KVSState a) IO BS.ByteString
decompressTable table = do
  (t, kvsstate') <- compute_ <$> get
  put kvsstate'
  return t
    where
      compute_ kvsstate@KVSState{_decompression=(Decompression decomp s)} =
                              let (t, s') = decomp s table
                              in (t,kvsstate{_decompression=Decompression decomp s})

store :: DB.DB_Iface a => T.Text -> Table -> StateT (KVSState a) IO ()
store tableId table = storeTable tableId =<< encryptTable =<< compressTable =<< serializeTable table

load :: DB.DB_Iface a => T.Text -> StateT (KVSState a) IO (Maybe (T.Text, Table))
load tableId = do
  serializedValTable <- loadTable tableId
  case serializedValTable of
    Nothing -> return Nothing
    (Just v) -> Just . (tableId,) <$> (deserializeTable =<< decryptTable =<< decompressTable v)
