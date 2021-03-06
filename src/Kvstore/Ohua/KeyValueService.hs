{-# LANGUAGE FlexibleContexts #-}

module Kvstore.Ohua.KeyValueService where

import           KeyValueStore_Iface
import           Kvservice_Types
import qualified Data.Text.Lazy                       as T
import qualified Data.HashSet                         as Set
import qualified Data.HashMap.Strict                  as Map
import           Control.Monad.State
import           Data.IORef
import qualified Data.Vector                          as Vector
import           Data.Maybe
import           Data.Int

import qualified Kvstore.Cache                        as Cache
import           Kvstore.KVSTypes

import qualified DB_Iface                             as DB
import           Debug.Trace

import           Kvstore.Ohua.KVSTypes

foldEvictFromCache :: KVStore -> Vector.Vector KVRequest -> StateT Stateless IO KVStore
foldEvictFromCache cache = return . foldl (\c r -> Map.delete (kVRequest_table r) c) cache . Cache.findWrites

foldIntoCache :: KVStore -> [Maybe (T.Text, Table)] -> StateT Stateless IO KVStore
foldIntoCache =
  foldM (\c e ->
            case e of
              (Just entry) -> do
                (_, KVSState c' _ _ _) <- liftIO $ runStateT (Cache.insertTableIntoCache entry) $ KVSState c undefined undefined undefined
                return c'
              Nothing -> return c)

foldINSERTsIntoCache :: KVStore -> [KVRequest] -> StateT Stateless IO KVStore
foldINSERTsIntoCache =
  foldM (\c req -> do
                (_, KVSState c' _ _ _) <- liftIO $ runStateT (Cache.mergeINSERTIntoCache req) $ KVSState c undefined undefined undefined
                return c')
