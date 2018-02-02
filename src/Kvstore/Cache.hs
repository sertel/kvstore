{-# LANGUAGE FlexibleContexts, TupleSections #-}

module Kvstore.Cache where

import           Kvservice_Types

import qualified Data.Text.Lazy         as T
import           Control.Monad.State
import qualified Data.Vector            as Vector
import qualified Data.HashMap.Strict    as Map
import           Data.Maybe

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

loadCacheEntry :: (DB.DB_Iface a, SerDe b) => T.Text -> StateT (KVSState a b) IO (T.Text, Table)
loadCacheEntry tableId = do
  (KVSState kvs db serde) <- get
  if Map.member tableId kvs
    then return (tableId, fromJust $ Map.lookup tableId kvs) -- redundant for now
    else do
      serializedValTable <- loadTable tableId
      valTable <- deserializeTable serializedValTable
      return (tableId, valTable)

-- TODO cache entry eviction etc. -> needs even more state to be stored

updateCache :: Vector.Vector (T.Text, Table) -> StateT (KVSState a b) IO ()
updateCache newEntries = (\_ -> return ()) =<< mapM updateCacheEntry newEntries
  where
    updateCacheEntry (tableId, valTable) = do
      (KVSState kvs db serde) <- get
      -- FIXME these might have to be merged in!
      let kvs' = Map.insert tableId valTable kvs
      put $ KVSState kvs' db serde

refreshCache :: (DB.DB_Iface a, SerDe b) => Vector.Vector KVRequest -> StateT (KVSState a b) IO ()
refreshCache reqs = do
  let readsAndScans = Vector.filter findReadsAndScans reqs
  let reads_ = Vector.map kVRequest_table readsAndScans
  newEntriesFromReads <- mapM loadCacheEntry reads_

  -- TODO different strategies are possible here
  -- let writes = Vector.filter findWrites reqs
  -- let neededWrites = Vector.filter (findNeeded readsAndScans) writes

  updateCache newEntriesFromReads

  where
    findReadsAndScans (KVRequest READ _ _ _ _ _) = True
    findReadsAndScans (KVRequest SCAN _ _ _ _ _) = True
    findReadsAndScans (KVRequest _ _ _ _ _ _) = False
    findWrites (KVRequest INSERT _ _ _ _ _) = True
    findWrites (KVRequest _ _ _ _ _ _) = False
    findNeeded readReqs writeReq =
      let
        writeTableId = kVRequest_table writeReq
        readsNeedWriteTable = Vector.filter ((writeTableId ==) . kVRequest_table) readReqs
      in
        -- what if these tables are not even present yet in the cache?! -> they will be after loading for the reads!
        -- all we need to do is make sure is that the order of reads and writes does not change! -> tricky
        --
        -- also: adding something to the cache before it was written to stable storage might result in
        --       an inconsistent state. -> we need to require the backend to always be able to write successfully!
        False -- TODO
