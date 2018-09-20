{-# LANGUAGE TupleSections, ConstraintKinds, TypeSynonymInstances,
  FlexibleInstances, LambdaCase #-}

module Kvstore.RequestHandling where

import           Control.Monad.State
import           Control.Lens

import           KeyValueStore_Iface
import           Kvservice_Types
import qualified Data.Text.Lazy          as T
import qualified Data.HashSet            as Set
import qualified Data.HashMap.Strict     as HM
import           Data.IORef
import qualified Data.Vector             as Vector
import           Data.Maybe
import           Data.Int
import qualified Data.List               as List
import           Data.Function
import LazyObject

import           Kvstore.KVSTypes
import qualified Kvstore.Cache           as Cache
import qualified Kvstore.InputOutput     as InOut

import qualified DB_Iface                as DB
import           Debug.Trace

type HasCache s m = (MonadState s m, AccessCache s)


class AccessCache s where
    getCache :: Lens' s KVStore


instance AccessCache (KVSState a) where
    getCache = cache

instance AccessCache KVStore where
    getCache = id

read_ :: HasCache s m => T.Text -> T.Text -> Maybe (Set.HashSet T.Text) -> m KVResponse
read_ _ _ Nothing = return $ KVResponse READ (Just HM.empty) Nothing Nothing
read_ table key (Just fields) =
    (HM.lookup table <$> use getCache) >>= \case
        Nothing ->
            return $
            KVResponse READ Nothing Nothing $ Just $ T.pack "no such table!"
        (Just valTable) ->
            case HM.lookup key valTable of
                Nothing ->
                    return $
                    KVResponse READ Nothing Nothing $
                    Just $ T.pack "no such key!"
                (Just fieldVals) ->
                    return $
                    KVResponse
                        READ
                        (Just $ findFields (LazyObject.read fieldVals) fields)
                        Nothing
                        Nothing
  where
    findFields fieldVals =
        HM.fromList .
        mapMaybe (\k -> (k, ) <$> HM.lookup k fieldVals) . Set.toList

scan :: HasCache s m => T.Text -> T.Text -> Maybe Int32 -> m KVResponse
scan _ _ Nothing =
    return $
    KVResponse SCAN Nothing Nothing $ Just $ T.pack "no record count specified!"
scan table key (Just recordCount) =
    (HM.lookup table <$> use getCache) >>= \case
        Nothing ->
            return $
            KVResponse SCAN Nothing Nothing $ Just $ T.pack "no such key!"
        (Just valTable) -> do
            let collected =
                    (Vector.fromList .
                     collect . List.sortBy (compare `on` fst) . HM.toList)
                        $ valTable
            return $ KVResponse SCAN Nothing (Just $ fmap LazyObject.read collected) Nothing
  where
    collect [] = []
    collect a@((k, _):xs)
        | k == key = (map snd . take (fromIntegral recordCount)) a
        | otherwise = collect xs

update :: (DB.DB_Iface a) => T.Text -> T.Text -> Maybe (HM.HashMap T.Text T.Text) -> StateT (KVSState a) IO KVResponse
update table key Nothing = update table key $ Just HM.empty
update tableId key (Just values) = do
  table <- use $ cache . at tableId . non mempty
  let table' = at key . non mempty . lazyO %~ HM.union values $ table
  InOut.store tableId table'
  return $ KVResponse UPDATE (Just HM.empty) Nothing Nothing

insert :: (DB.DB_Iface a) => T.Text -> T.Text -> Maybe (HM.HashMap T.Text T.Text) -> StateT (KVSState a) IO KVResponse
insert table key Nothing = insert table key $ Just HM.empty
insert tableId key (Just values) = do
  table <- use $ cache . at tableId . non mempty
  let table' = at key ?~ newChanged values $ table
  InOut.store tableId table'
  return $ KVResponse INSERT (Just HM.empty) Nothing Nothing

-- TODO The delete doesn't actually work ... I don't know why sebastian didn't implement it
delete :: (DB.DB_Iface a) => T.Text -> T.Text -> StateT (KVSState a) IO KVResponse
delete tableId key = do
  cache . ix tableId . at key .= Nothing
  return $ KVResponse DELETE (Just HM.empty) Nothing Nothing

serve :: HasCache s m => KVRequest -> m KVResponse
serve (KVRequest op table key fields recordCount values) =
  case op of
    READ   -> read_ table key fields
    SCAN   -> scan table key recordCount
    a -> pure $ KVResponse a (Just mempty) Nothing Nothing

writeback :: DB.DB_Iface db => Set.HashSet T.Text -> StateT (KVSState db) IO ()
writeback = mapM_ write . Set.toList
  where
    write tableId =
        preuse (cache . ix tableId) >>= InOut.store tableId . fromMaybe mempty
