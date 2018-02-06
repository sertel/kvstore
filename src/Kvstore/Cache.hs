{-# LANGUAGE FlexibleContexts, TupleSections #-}

module Kvstore.Cache where

import           Kvservice_Types

import qualified Data.Text.Lazy          as T
import qualified Data.ByteString.Lazy    as BS
import           Control.Monad.State
import qualified Data.Vector             as Vector
import qualified Data.HashMap.Strict     as Map
import           Data.Maybe

import qualified DB_Iface                as DB
import           Kvstore.KVSTypes
import           Kvstore.InputOutput

import           Debug.Trace

loadCacheEntry :: (DB.DB_Iface a, SerDe b) => T.Text -> StateT (KVSState a b) IO (T.Text, Table)
loadCacheEntry tableId = do
  (KVSState kvs db serde) <- get
  case Map.lookup tableId kvs of
      (Just table) -> return (tableId, table)
      Nothing -> do
                    serializedValTable <- loadTable tableId
                    case serializedValTable of
                      Nothing -> return (tableId, Map.empty)
                      (Just v) -> (return . (tableId,)) =<< deserializeTable v


-- TODO cache entry eviction etc. -> needs even more state to be stored

updateCache :: Vector.Vector (T.Text, Table) -> StateT (KVSState a b) IO ()
updateCache newEntries = (\_ -> return ()) =<< mapM updateCacheEntry newEntries
  where
    updateCacheEntry (tableId, table) = do
      (KVSState kvs db serde) <- get
      -- FIXME these might have to be merged in!
      let kvs' = Map.insert tableId table kvs
      put $ KVSState kvs' db serde

refresh :: (DB.DB_Iface a, SerDe b) => Vector.Vector KVRequest -> StateT (KVSState a b) IO ()
refresh reqs = do
  let reads_ = (Vector.map kVRequest_table . Vector.filter findReadsAndScans) reqs
  newEntriesFromReads <- mapM loadCacheEntry reads_

  -- we also load the entries for the writes because requests are on the granularity of a table
  -- and the request of the service are on the granularity of the table entries.
  let writes = (Vector.map kVRequest_table . Vector.filter findWrites) reqs
  newEntriesFromWrites <- mapM loadCacheEntry writes

    -- TODO different strategies are possible here
  -- let neededWrites = Vector.filter (findNeeded readsAndScans) writes

  updateCache newEntriesFromReads
  updateCache newEntriesFromWrites

  where
    findReadsAndScans (KVRequest READ _ _ _ _ _) = True
    findReadsAndScans (KVRequest SCAN _ _ _ _ _) = True
    findReadsAndScans (KVRequest _ _ _ _ _ _) = False
    findWrites (KVRequest INSERT _ _ _ _ _) = True
    findWrites (KVRequest UPDATE _ _ _ _ _) = True
    findWrites (KVRequest DELETE _ _ _ _ _) = True
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

update :: T.Text -> T.Text -> Maybe (Map.HashMap T.Text T.Text) -> StateT (KVSState a b) IO KVResponse
update table key Nothing = update table key $ Just Map.empty
update table key (Just values) = do
   (KVSState tables db serde) <- get
   case (Map.lookup table tables) of
     Nothing -> return $ KVResponse UPDATE Nothing Nothing $ Just $ T.pack "no such table!"
     (Just valTable) -> do
       case (Map.lookup key valTable) of
         Nothing -> return $ KVResponse UPDATE Nothing Nothing $ Just $ T.pack "no such key!"
         (Just fields) -> do
           let fields' =  Map.union values fields
           let valTable' = Map.insert key fields' valTable
           let kvs' = Map.insert table valTable' tables
           put $ KVSState kvs' db serde
           return $ KVResponse UPDATE (Just Map.empty) Nothing Nothing

insert :: T.Text -> T.Text -> Maybe (Map.HashMap T.Text T.Text) -> StateT (KVSState a b) IO KVResponse
insert table key Nothing = insert table key $ Just Map.empty
insert table key (Just values) = do
   (KVSState kvs db serde) <- get
   case (Map.lookup table kvs) of
     Nothing -> return $ KVResponse INSERT Nothing Nothing $ Just $ T.pack "no such table!"
     (Just valTable) -> do
                          let valTable' = Map.insert key values valTable
                          let kvs' = Map.insert table valTable' kvs
                          put $ KVSState kvs' db serde
                          return $ KVResponse INSERT (Just Map.empty) Nothing Nothing

delete :: T.Text -> T.Text -> StateT (KVSState a b) IO KVResponse
delete table key = do
  (KVSState kvs db serde) <- get
  case (Map.lookup table kvs) of -- probably something that should be done even before request processing
    Nothing -> return $ KVResponse DELETE Nothing Nothing $ Just $ T.pack "no such table!"
    (Just valTable) -> do
                         let valTable' = Map.delete key valTable
                         let kvs' = Map.adjust (\_ -> valTable') key kvs
                         put $ KVSState kvs' db serde
                         return $ KVResponse DELETE Nothing Nothing Nothing
