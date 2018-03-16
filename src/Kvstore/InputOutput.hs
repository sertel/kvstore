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
