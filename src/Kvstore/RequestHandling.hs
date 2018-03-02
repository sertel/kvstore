{-# LANGUAGE TupleSections #-}

module Kvstore.RequestHandling where

import           KeyValueStore_Iface
import           Kvservice_Types
import qualified Data.Text.Lazy          as T
import qualified Data.HashSet            as Set
import qualified Data.HashMap.Strict     as HM
import           Control.Monad.State
import           Data.IORef
import qualified Data.Vector             as Vector
import           Data.Maybe
import           Data.Int
import qualified Data.List               as List
import           Data.Function

import           Kvstore.KVSTypes
import qualified Kvstore.Cache           as Cache
import qualified Kvstore.InputOutput     as InOut

import qualified DB_Iface                as DB
import           Debug.Trace


read_ :: T.Text -> T.Text -> Maybe (Set.HashSet T.Text) -> StateT (KVSState a) IO KVResponse
read_ table key Nothing = return $ KVResponse READ (Just HM.empty) Nothing Nothing
read_ table key (Just fields) = do
  tables <- getKvs <$> get
  case HM.lookup table tables of
    Nothing -> return $ KVResponse READ Nothing Nothing $ Just $ T.pack "no such table!"
    (Just valTable) -> case HM.lookup key valTable of
                          Nothing -> return $ KVResponse READ Nothing Nothing $ Just $ T.pack "no such key!"
                          (Just fieldVals) -> return $ KVResponse
                                                         READ
                                                         (Just $ findFields fieldVals fields)
                                                         Nothing
                                                         Nothing
  where
    findFields fieldVals = HM.fromList . mapMaybe (\k -> (k,) <$> HM.lookup k fieldVals) . Set.toList

scan :: T.Text -> T.Text -> Maybe Int32 -> StateT (KVSState a) IO KVResponse
scan table key Nothing = return $ KVResponse SCAN Nothing Nothing $ Just $ T.pack "no record count specified!"
scan table key (Just recordCount) = do
  tables <- getKvs <$> get
  case HM.lookup table tables of
    Nothing -> return $ KVResponse SCAN Nothing Nothing $ Just $ T.pack "no such key!"
    (Just valTable) -> do
                        let collected = (Vector.fromList . collect . List.sortBy (compare `on` fst) . HM.toList) valTable
                        return $ KVResponse SCAN Nothing (Just collected) Nothing
  where
    collect [] = []
    collect a@((k,v):xs) | k == key = (map snd . take (fromIntegral recordCount)) a
                         | otherwise = collect xs

update :: (DB.DB_Iface a) => T.Text -> T.Text -> Maybe (HM.HashMap T.Text T.Text) -> StateT (KVSState a) IO KVResponse
update table key Nothing = update table key $ Just HM.empty
update tableId key (Just values) = do
  s <- get
  cache <- (return . getKvs) s
  let table = case HM.lookup tableId cache of { (Just t) -> t; Nothing -> HM.empty }
  let vals' = case HM.lookup key table of
              Nothing -> values
              (Just vals) -> HM.union values vals
  let table' = HM.insert key vals' table
  InOut.storeTable tableId =<< InOut.serializeTable table'
  return $ KVResponse UPDATE (Just HM.empty) Nothing Nothing

insert :: (DB.DB_Iface a) => T.Text -> T.Text -> Maybe (HM.HashMap T.Text T.Text) -> StateT (KVSState a) IO KVResponse
insert table key Nothing = insert table key $ Just HM.empty
insert tableId key (Just values) = do
  s <- get
  cache <- (return . getKvs) s
  let table' = case HM.lookup tableId cache of
                  (Just table) -> HM.insert key values table
                  Nothing -> HM.singleton key values
  InOut.storeTable tableId =<< InOut.serializeTable table'
  return $ KVResponse INSERT (Just HM.empty) Nothing Nothing

delete :: (DB.DB_Iface a) => T.Text -> T.Text -> StateT (KVSState a) IO KVResponse
delete tableId key = do
  s <- get
  cache <- (return . getKvs) s
  case HM.lookup tableId cache of
        (Just table) -> InOut.storeTable tableId =<< InOut.serializeTable (HM.delete key table)
        Nothing -> return ()
  return $ KVResponse DELETE (Just HM.empty) Nothing Nothing

serve :: (DB.DB_Iface a) => KVRequest -> StateT (KVSState a) IO KVResponse
serve (KVRequest op table key fields recordCount values) =
  case op of
    READ   -> read_ table key fields
    SCAN   -> scan table key recordCount
    UPDATE -> update table key values
    INSERT -> insert table key values
    DELETE -> delete table key
