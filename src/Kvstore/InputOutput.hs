

module Kvstore.InputOutput where

import qualified Data.Text.Lazy          as T
import           Control.Monad.State
import qualified Data.ByteString.Lazy    as BS
import qualified Data.Text.Lazy.Encoding as Enc

import qualified DB_Iface                as DB
import           Db_Types
import           Kvstore.KVSTypes


loadTable :: DB.DB_Iface a => T.Text -> StateT (KVSState a) IO (Maybe BS.ByteString)
loadTable tableId = do
  (KVSState _ db _ _) <- get
  serializedValTable <- liftIO $ DB.get db tableId
  case serializedValTable of
    (DBResponse Nothing) -> return Nothing
    (DBResponse (Just v)) -> return $ Just $ Enc.encodeUtf8 v

deserializeTable :: BS.ByteString -> StateT (KVSState a) IO Table
deserializeTable serializedTable = do
  (KVSState cache db ser (Deserialization deser s)) <- get
  let (t, s') = deser s serializedTable
  put $ KVSState cache db ser $ Deserialization deser s'
  return t

storeTable :: DB.DB_Iface a => T.Text -> BS.ByteString -> StateT (KVSState a) IO ()
storeTable key serializedTable = do
  db <- getDbBackend <$> get
  _ <- liftIO $ DB.put db key $ Enc.decodeUtf8 serializedTable
  return ()

serializeTable :: Table -> StateT (KVSState a) IO BS.ByteString
serializeTable table = do
  (KVSState cache db (Serialization ser s) deser) <- get
  let (t, s') = ser s table
  put $ KVSState cache db (Serialization ser s') deser
  return t
