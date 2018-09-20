{-# LANGUAGE FlexibleContexts, TupleSections #-}

module Kvstore.Cache where

import           Kvservice_Types
import           Control.Monad.State
import           Control.Lens
import qualified Data.Text.Lazy          as T
import qualified Data.ByteString.Lazy    as BS
import qualified Data.Vector             as Vector
import qualified Data.HashMap.Strict     as Map
import qualified Data.HashSet            as HS
import           Data.Maybe

import qualified DB_Iface                as DB
import           Kvstore.KVSTypes
import           Kvstore.InputOutput

import LazyObject

import           Debug.Trace


-- TODO cache entry eviction etc. -> needs even more state to be stored

findReads :: Vector.Vector KVRequest -> Vector.Vector KVRequest
findReads = Vector.filter (flip HS.member (HS.fromList [READ,SCAN]) . kVRequest_op)

findWrites :: Vector.Vector KVRequest -> Vector.Vector KVRequest
findWrites = Vector.filter (flip HS.member (HS.fromList [INSERT,UPDATE,DELETE]) . kVRequest_op)

findInserts :: Vector.Vector KVRequest -> Vector.Vector KVRequest
findInserts = Vector.filter (flip HS.member  (HS.fromList [INSERT]) . kVRequest_op)

loadCacheEntry :: (DB.DB_Iface a) => T.Text -> StateT (KVSState a) IO (Maybe (T.Text, Table))
loadCacheEntry tableId = do
  kvsstate <- get
  let kvs = view cache kvsstate
  case Map.lookup tableId kvs of
      (Just table) -> return $ Just (tableId, table)
      Nothing -> load tableId

insertTableIntoCache :: (T.Text, Table) -> StateT (KVSState a) IO ()
insertTableIntoCache (tableId, table) = cache %= Map.insert tableId table

mergeIntoCache :: T.Text -> T.Text -> Maybe (Map.HashMap T.Text T.Text) -> StateT (KVSState a) IO ()
mergeIntoCache tableId key Nothing = mergeIntoCache tableId key $ Just Map.empty
mergeIntoCache tableId key (Just values) =
  cache . at tableId . non mempty . at key .= Just (newChanged values)

mergeINSERTIntoCache :: KVRequest -> StateT (KVSState a) IO ()
mergeINSERTIntoCache (KVRequest op table key fields recordCount values) =
  case op of
    INSERT -> mergeIntoCache table key values
    _ -> error "invariant broken"

updateCache :: [(T.Text, Table)] -> StateT (KVSState a) IO ()
updateCache newEntries = (\_ -> return ()) =<< mapM insertTableIntoCache newEntries

refresh :: (DB.DB_Iface a) => Vector.Vector KVRequest -> StateT (KVSState a) IO ()
refresh reqs = do
  let reads_ = (HS.toList . HS.fromList . map kVRequest_table . Vector.toList . findReads) reqs
  newEntriesFromReads <- mapM loadCacheEntry reads_

  -- we also load the entries for the writes because requests are on the granularity of a table
  -- and the request of the service are on the granularity of the table entries.
  let writes = (HS.toList . HS.fromList . map kVRequest_table . Vector.toList . findWrites) reqs
  newEntriesFromWrites <- mapM loadCacheEntry writes

    -- TODO different strategies are possible here
  -- let neededWrites = Vector.filter (findNeeded readsAndScans) writes

  update_ newEntriesFromReads
  update_ newEntriesFromWrites

  -- (\x -> traceM $ "updated cache: " ++ show (getKvs x)) =<< get
  where
    update_ = updateCache . catMaybes
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

update :: T.Text -> T.Text -> Maybe (Map.HashMap T.Text T.Text) -> StateT (KVSState a) IO KVResponse
update table key Nothing = update table key $ Just Map.empty
update table key (Just values) = do
   kvsstate <- get
   let kvs = view cache kvsstate
   case Map.lookup table kvs of
     Nothing -> return $ KVResponse UPDATE Nothing Nothing $ Just $ T.pack "no such table!"
     (Just valTable) ->
       case Map.lookup key valTable of
         Nothing -> return $ KVResponse UPDATE Nothing Nothing $ Just $ T.pack "no such key!"
         (Just fields) -> do
           let fields' =  write (Map.union values) fields
           let valTable' = Map.insert key fields' valTable
           let kvs' = Map.insert table valTable' kvs
           cache .= kvs'
           return $ KVResponse UPDATE (Just Map.empty) Nothing Nothing


insert :: T.Text -> T.Text -> Maybe (Map.HashMap T.Text T.Text) -> StateT (KVSState a) IO KVResponse
insert table key Nothing = insert table key $ Just Map.empty
insert table key (Just values) = do
    kvsstate <- get
    let kvs = view cache kvsstate
    if Map.member table kvs
        then return $ KVResponse INSERT Nothing Nothing $ Just $
             T.pack "no such table!"
        else do
            cache . ix table . at key .= Just (newChanged values)
            return $ KVResponse INSERT (Just Map.empty) Nothing Nothing

delete :: T.Text -> T.Text -> StateT (KVSState a) IO KVResponse
delete table key = do
  kvsstate <- get
  let kvs = view cache kvsstate
  if Map.member table kvs  -- probably something that should be done even before request processing
    then return $ KVResponse DELETE Nothing Nothing $ Just $ T.pack "no such table!"
    else do
      cache . ix table . at key .= Nothing
      return $ KVResponse DELETE Nothing Nothing Nothing

invalidateReq :: KVRequest -> StateT (KVSState a) IO ()
invalidateReq req = do
    kvsstate <- get
    let kvs = view cache kvsstate
        kvs' = Map.delete (kVRequest_table req) kvs
    put $ over cache (const kvs') kvsstate

-- very coarse-grained, I know
invalidate :: Vector.Vector KVRequest -> StateT (KVSState a) IO ()
invalidate = mapM_  invalidateReq . findWrites
