
module Kvstore.Cache where

import           Kvservice_Types

import qualified Data.Text.Lazy         as T
import           Control.Monad.State
import qualified Data.Vector            as Vector
import qualified Data.HashMap.Strict    as Map

import qualified DB_Iface               as DB
import           Kvstore.KVSTypes

loadTable :: DB.DB_Iface a => T.Text -> StateT (KVSState a b) IO T.Text
loadTable tableId = do
  (KVSState _ db _) <- get
  serializedValTable <- liftIO $ DB.get db tableId
  return serializedValTable

deserializeTable :: SerDe b => T.Text -> StateT (KVSState a b) IO Table
deserializeTable serializedTable = do
  (KVSState _ _ serializer) <- get
  return $ deserialize serializer serializedTable

refreshCacheEntry :: (DB.DB_Iface a, SerDe b) => T.Text -> StateT (KVSState a b) IO ()
refreshCacheEntry tableId = do
  (KVSState kvs db serde) <- get
  if Map.member tableId kvs
    then return ()
    else do
      serializedValTable <- loadTable tableId
      valTable <- deserializeTable serializedValTable
      let kvs' = Map.insert tableId valTable kvs
      _ <- put $ KVSState kvs' db serde
      return ()

refreshCache :: (DB.DB_Iface a, SerDe b) => Vector.Vector KVRequest -> StateT (KVSState a b) IO ()
refreshCache reqs = do
  let reads_ = ((Vector.map kVRequest_table) . (Vector.filter findReadsAndScans)) reqs
  _ <- mapM refreshCacheEntry reads_
  return ()
  where
    findReadsAndScans (KVRequest READ _ _ _ _ _) = True
    findReadsAndScans (KVRequest SCAN _ _ _ _ _) = True
    findReadsAndScans (KVRequest _ _ _ _ _ _) = False
