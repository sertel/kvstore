

module Kvstore.InputOutput where

import qualified Data.Text.Lazy          as T
import           Control.Monad.State
import qualified Data.ByteString.Lazy    as BS
import qualified Data.Text.Lazy.Encoding as Enc

import qualified DB_Iface                as DB
import           Db_Types
import           Kvstore.KVSTypes


loadTable :: DB.DB_Iface a => T.Text -> StateT (KVSState a b) IO (Maybe BS.ByteString)
loadTable tableId = do
  (KVSState _ db _) <- get
  serializedValTable <- liftIO $ DB.get db tableId
  case serializedValTable of
    (DBResponse Nothing) -> return Nothing
    (DBResponse (Just v)) -> return $ Just $ Enc.encodeUtf8 v

deserializeTable :: SerDe b => BS.ByteString -> StateT (KVSState a b) IO Table
deserializeTable serializedTable = do
  (KVSState _ _ serializer) <- get
  return $ deserialize serializer serializedTable

storeTable :: DB.DB_Iface a => T.Text -> BS.ByteString -> StateT (KVSState a b) IO ()
storeTable key serializedTable = do
  db <- (return . getDbBackend) =<< get
  _ <- liftIO $ DB.put db key $ Enc.decodeUtf8 serializedTable
  return ()

serializeTable :: SerDe b => Table -> StateT (KVSState a b) IO BS.ByteString
serializeTable table = do
  (KVSState _ _ serializer) <- get
  return $ serialize serializer table
