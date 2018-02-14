{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}

module Kvstore.KeyValueService where

import           KeyValueStore_Iface
import           Kvservice_Types
import qualified Data.Text.Lazy          as T
import qualified Data.HashSet            as Set
import qualified Data.HashMap.Strict     as Map
import           Control.Monad.State
import           Data.IORef
import qualified Data.Vector             as Vector
import           Data.Maybe
import           Data.Int

import qualified Kvstore.Cache           as Cache
import qualified Kvstore.RequestHandling as RH
import           Kvstore.KVSTypes

import qualified DB_Iface                as DB
import           Debug.Trace

-- almost purely functional version except for the fact that it uses an imperative
-- mapM to update the cache.
execRequestsFuncImp :: (DB.DB_Iface a, SerDe b) => Vector.Vector KVRequest -> StateT (KVSState a b) IO (Vector.Vector KVResponse)
execRequestsFuncImp reqs = do
  (KVSState cache db serde) <- get

  -- cache management: load all entries needed to process the requests
  (newEntries, KVSState _ db' serde') <- liftIO $ runStateT (mapM Cache.loadCacheEntry [kVRequest_table req | req <- Vector.toList reqs])
                                                            $ KVSState cache db serde

  -- mapM here actually folds over the cache! (mapM is a sequential 'for-loop' over the state by definition!)
  (_, KVSState cache' _ _) <- runStateT (mapM (\case
                                                  (Just entry) -> do
                                                    -- I wrote this very explicitly here to show what happens with the state.
                                                    -- one could also just write:
                                                    -- Cache.updateCacheEntry entry
                                                    s <- get
                                                    (_,s') <- liftIO $ runStateT (Cache.updateCacheEntry entry) s
                                                    put s'
                                                  Nothing -> return ())
                                          newEntries)
                                          $ KVSState cache undefined undefined

  -- request handling
  (responses, KVSState _ db'' serde'') <- liftIO $ runStateT (mapM RH.serve reqs) $ KVSState cache' db' serde'

  -- cache management: propagate side-effects to cache
  (_, KVSState cache'' _ _) <- liftIO $ runStateT ((mapM_  Cache.invalidateReq . Cache.findWrites) reqs) $ KVSState cache' undefined undefined

  put $ KVSState cache'' db'' serde''
  return responses

-- the purely functional version explicitly folds over the cache!
execRequestsFunctional :: (DB.DB_Iface a, SerDe b) => Vector.Vector KVRequest -> StateT (KVSState a b) IO (Vector.Vector KVResponse)
execRequestsFunctional reqs = do
  -- cache management: load all entries needed to process the requests
  (KVSState cache db serde) <- get
  (newEntries, KVSState _ db' serde') <- liftIO $ runStateT (mapM Cache.loadCacheEntry [kVRequest_table req | req <- Vector.toList reqs])
                                                              $ KVSState cache db serde

  cache' <- foldM (\c e ->
                    case e of
                      (Just entry) -> do
                        (_, KVSState c' _ _) <- liftIO $ runStateT (Cache.updateCacheEntry entry) $ KVSState c undefined undefined
                        return c'
                      Nothing -> return c)
                  cache newEntries

  -- request handling
  (responses, KVSState _ db'' serde'') <- liftIO $ runStateT (mapM RH.serve reqs) $ KVSState cache' db' serde'

  -- cache management: propagate side-effects to cache
  (_, KVSState cache'' _ _) <- liftIO $ runStateT ((mapM_  Cache.invalidateReq . Cache.findWrites) reqs) $ KVSState cache' undefined undefined

  put $ KVSState cache'' db'' serde''
  return responses

-- coarse-grained:
-- this is the imperative version of the algorithm that places the state in the cache.
-- turning this into a parallel version would not work because of the different uses of
-- the cache and the db connections.
execRequestsCoarse :: (DB.DB_Iface a, SerDe b) => Vector.Vector KVRequest -> StateT (KVSState a b) IO (Vector.Vector KVResponse)
execRequestsCoarse reqs = do
  -- cache management: load all entries needed to process the requests
  Cache.refresh reqs

  -- request handling
  responses <- mapM RH.serve reqs
  -- (\x -> traceM $ "cache after request handling: " ++ show x) . getKvs =<< get

  -- cache management: propagate side-effects to cache
  Cache.invalidate reqs
  -- (\x -> traceM $ "cache after invalidating: " ++ show x) . getKvs =<< get

  return responses

-- fine-grained:
-- same as the above but lifting more functionality into this function.
execRequestsFine :: (DB.DB_Iface a, SerDe b) => Vector.Vector KVRequest -> StateT (KVSState a b) IO (Vector.Vector KVResponse)
execRequestsFine reqs = do
  -- cache management: load all entries needed to process the requests
  newEntries <- mapM Cache.loadCacheEntry [kVRequest_table req | req <- Vector.toList reqs]
  (\_ -> return ()) =<< (mapM Cache.updateCacheEntry . catMaybes) newEntries

  -- request handling
  responses <- mapM RH.serve reqs
  -- (\x -> traceM $ "cache after request handling: " ++ show x) . getKvs =<< get

  -- cache management: propagate side-effects to cache
  (mapM_  Cache.invalidateReq . Cache.findWrites) reqs
  -- (\x -> traceM $ "cache after invalidating: " ++ show x) . getKvs =<< get

  return responses


instance (DB.DB_Iface a, SerDe b) => KeyValueStore_Iface (KVSHandler a b) where
  requests :: KVSHandler a b -> Vector.Vector KVRequest -> IO (Vector.Vector KVResponse)
  requests (KVSHandler stateRef) reqs = do
    state <- readIORef stateRef
    (responses,state') <- runStateT (execRequestsCoarse reqs) state
    _ <- writeIORef stateRef state'
    return responses
