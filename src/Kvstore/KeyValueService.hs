{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase, GADTs, FlexibleContexts, RankNTypes #-}

module Kvstore.KeyValueService where

import           KeyValueStore_Iface
import           Kvservice_Types
import           Control.Monad.State
import           Control.Lens
import qualified Data.Text.Lazy          as T
import qualified Data.HashSet            as Set
import qualified Data.HashMap.Strict     as Map
import           Data.IORef
import qualified Data.Vector             as Vector
import           Data.Maybe
import           Data.Int
import Data.Foldable (foldlM)

import qualified Kvstore.Cache           as Cache
import qualified Kvstore.RequestHandling as RH
import           Kvstore.KVSTypes

import qualified DB_Iface                as DB
import           Debug.Trace


idLens :: Lens a b a b
idLens = ($)

foldWritesIntoCache :: Foldable f => KVStore -> f KVRequest -> (Set.HashSet T.Text, KVStore)
foldWritesIntoCache store reqs =
    runState (foldWritesIntoCacheS idLens reqs) store

foldWritesIntoCacheS ::
       (MonadState s m, Foldable f)
    => Lens' s KVStore
    -> f KVRequest
    -> m (Set.HashSet T.Text)
foldWritesIntoCacheS l = foldlM integrateWrite mempty
  where
    integrateWrite touched (KVRequest op tableId key fields recCount mvalues) = do
        l . at tableId %= Just . fromMaybe mempty
        case op of
            DELETE -> tableL . at key .= Nothing
            _ ->
                case mvalues of
                    Just values ->
                        case op of
                            UPDATE -> tableL . ix key %= (values `Map.union`)
                            INSERT -> tableL . at key .= Just values
                            other ->
                                error $ "invalid operation in fold writes " ++
                                show other
                    Nothing -> return ()
        return $ Set.insert tableId touched
      where
        tableL = l . ix tableId

-- almost purely functional version except for the fact that it uses an imperative
-- mapM to update the cache.
execRequestsFuncImp ::
       (DB.DB_Iface a)
    => Vector.Vector KVRequest
    -> StateT (KVSState a) IO (Vector.Vector KVResponse)
execRequestsFuncImp reqs = do
    s0@(KVSState cache_ db ser deser comp decomp enc dec) <- get
  -- cache management: load all entries needed to process the requests
    (newEntries, s1@(KVSState { _storage = db'
                              , _deserializer = deser'
                              , _decompression = decomp'
                              , _decryption = dec
                              })) <-
        liftIO $
        runStateT
            (mapM
                 Cache.loadCacheEntry
                 [kVRequest_table req | req <- Vector.toList reqs])
            s0
  -- mapM here actually folds over the cache! (mapM is a sequential 'for-loop' over the state by definition!)
    s2@(KVSState {_cache = cache'}) <-
        execStateT
            (mapM
                 (\case
                      (Just entry)
                        -- I wrote this very explicitly here to show what happens with the state.
                        -- one could also just write:
                        -- Cache.updateCacheEntry entry
                       -> do
                          s <- get
                          (_, s') <-
                              liftIO $
                              runStateT (Cache.insertTableIntoCache entry) s
                          put s'
                      Nothing -> return ())
                 newEntries) $
        s1 {_cache = cache_}
  -- request handling
    (responses, s3@(KVSState _ db'' ser' _ comp' _ enc' _)) <-
        liftIO $
        runStateT (mapM RH.serve reqs) $
        KVSState
            { _cache = cache'
            , _storage = db'
            , _serializer = ser
            , _compression = comp
            , _encryption = enc
            }
    KVSState {_storage = db'''} <-
        liftIO $
        flip execStateT s3 $
        foldWritesIntoCacheS cache (Cache.findWrites reqs) >>=
        RH.writeback
  -- cache management: propagate side-effects to cache
    KVSState {_cache = cache''} <-
        liftIO $
        execStateT ((mapM_ Cache.invalidateReq . Cache.findWrites) reqs) $
        KVSState {_cache = cache'}
    put $ KVSState cache'' db''' ser' deser' comp' decomp' enc dec
    return responses

-- the purely functional version explicitly folds over the cache!
execRequestsFunctional ::
       (DB.DB_Iface a)
    => Vector.Vector KVRequest
    -> StateT (KVSState a) IO (Vector.Vector KVResponse)
execRequestsFunctional reqs
  -- cache management: load all entries needed to process the requests
 = do
    (KVSState cache_ db ser deser comp decomp enc dec) <- get
    (newEntries, KVSState { _storage = db'
                          , _deserializer = deser'
                          , _decompression = decomp'
                          , _decryption = dec'
                          }) <-
        liftIO $
        runStateT
            (mapM
                 Cache.loadCacheEntry
                 [kVRequest_table req | req <- Vector.toList reqs])
            KVSState
                { _cache = cache_
                , _storage = db
                , _deserializer = deser
                , _decompression = decomp
                , _decryption = dec
                }
    cache' <-
        foldM
            (\c e ->
                 case e of
                     (Just entry) -> do
                         (_, KVSState {_cache = c'}) <-
                             liftIO $
                             runStateT
                                 (Cache.insertTableIntoCache entry)
                                 KVSState {_cache = c}
                         return c'
                     Nothing -> return c)
            cache_
            newEntries
  -- request handling
    (responses, KVSState { _storage = db''
                         , _serializer = ser'
                         , _compression = comp'
                         , _encryption = enc'
                         }) <-
        liftIO $
        runStateT
            (mapM RH.serve reqs)
            KVSState
                { _cache = cache'
                , _storage = db'
                , _serializer = ser
                , _compression = comp
                , _encryption = enc
                }
  -- cache management: propagate side-effects to cache
    KVSState { _storage = db''' } <-
        liftIO $
        flip
            execStateT
            (KVSState
                 { _cache = cache'
                 , _compression = comp'
                 , _serializer = ser'
                 , _decompression = decomp'
                 , _encryption = enc'
                 , _storage = db''
                 }) $ do
            touched <-
                foldWritesIntoCacheS cache $ Cache.findWrites reqs
            RH.writeback touched
    KVSState {_cache = cache'''} <-
        liftIO $
        execStateT
            ((mapM_ Cache.invalidateReq . Cache.findWrites) reqs)
            KVSState {_cache = cache'}
    put $ KVSState cache''' db''' ser' deser' comp' decomp' enc dec
    return responses

-- coarse-grained:
-- this is the imperative version of the algorithm that places the state in the cache.
-- turning this into a parallel version would not work because of the different uses of
-- the cache and the db connections.
execRequestsCoarse ::
       (DB.DB_Iface a)
    => Vector.Vector KVRequest
    -> StateT (KVSState a) IO (Vector.Vector KVResponse)
execRequestsCoarse reqs = do
  -- cache management: load all entries needed to process the requests
  Cache.refresh reqs
  -- request handling
  responses <- mapM RH.serve reqs
  -- (\x -> traceM $ "cache after request handling: " ++ show x) . getKvs =<< get

  touched <- foldWritesIntoCacheS cache $ Cache.findWrites reqs

  RH.writeback touched

  -- cache management: propagate side-effects to cache
  Cache.invalidate reqs
  -- (\x -> traceM $ "cache after invalidating: " ++ show x) . getKvs =<< get

  return responses

-- fine-grained:
-- same as the above but lifting more functionality into this function.
execRequestsFine ::
       (DB.DB_Iface a)
    => Vector.Vector KVRequest
    -> StateT (KVSState a) IO (Vector.Vector KVResponse)
execRequestsFine reqs = do
  -- cache management: load all entries needed to process the requests
  newEntries <- mapM Cache.loadCacheEntry [kVRequest_table req | req <- Vector.toList reqs]
  mapM_ Cache.insertTableIntoCache $ catMaybes newEntries

  -- request handling
  responses <- mapM RH.serve reqs
  -- (\x -> traceM $ "cache after request handling: " ++ show x) . getKvs =<< get

  touched <- foldWritesIntoCacheS cache $ Cache.findWrites reqs

  RH.writeback touched

  -- cache management: propagate side-effects to cache
  mapM_ Cache.invalidateReq $ Cache.findWrites reqs
  -- (\x -> traceM $ "cache after invalidating: " ++ show x) . getKvs =<< get

  return responses


instance (DB.DB_Iface a) => KeyValueStore_Iface (KVSHandler a) where
    requests ::
           KVSHandler a
        -> Vector.Vector KVRequest
        -> IO (Vector.Vector KVResponse)
    requests (KVSHandler stateRef) reqs = do
        state <- readIORef stateRef
        (responses, state') <- runStateT (execRequestsCoarse reqs) state
        _ <- writeIORef stateRef state'
        return responses
