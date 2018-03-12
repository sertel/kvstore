{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}

module Kvstore.Ohua.RequestHandling where

import           Data.Int
import qualified Data.Text.Lazy             as T
import qualified Data.HashSet               as Set
import qualified Data.HashMap.Strict     as HM
import qualified Data.ByteString.Lazy    as BS
import           Data.Maybe
import           Control.Monad.State
import           Control.Monad.IO.Class

import           KeyValueStore_Iface
import           Kvservice_Types

import qualified DB_Iface                   as DB
import           Kvstore.KVSTypes
import qualified Kvstore.RequestHandling    as RH
import qualified Kvstore.InputOutput        as InOut

import           Kvstore.Ohua.KVSTypes
import           Debug.Trace


read_ :: KVStore -> T.Text -> T.Text -> Maybe (Set.HashSet T.Text) -> StateT Stateless IO KVResponse
read_ cache tableId key fields = liftIO . flip evalStateT (KVSState cache undefined undefined undefined) $ RH.read_ tableId key fields
-- FIXME Why is this not working?!
-- read_ cache = liftIO . flip evalStateT (KVSState cache undefined undefined) . RH.read_

scan :: KVStore -> T.Text -> T.Text -> Maybe Int32 -> StateT Stateless IO KVResponse
scan cache tableId key count = liftIO . flip evalStateT (KVSState cache undefined undefined undefined) $ RH.scan tableId key count

calculateUpdate :: KVStore -> T.Text -> T.Text -> HM.HashMap T.Text T.Text -> StateT Stateless IO Table
calculateUpdate cache tableId key values =
  let table = case HM.lookup tableId cache of { (Just t) -> t; Nothing -> HM.empty }
      vals' = case HM.lookup key table of
            Nothing -> values
            (Just vals) -> HM.union values vals
      table' = HM.insert key vals' table
  in return table'

calculateInsert :: KVStore -> T.Text -> T.Text -> HM.HashMap T.Text T.Text -> StateT Stateless IO Table
calculateInsert cache tableId key values = return $ case HM.lookup tableId cache of
                                                      (Just table) -> HM.insert key values table
                                                      Nothing -> HM.singleton key values

calculateDelete :: KVStore -> T.Text -> T.Text -> HM.HashMap T.Text T.Text -> StateT Stateless IO Table
calculateDelete cache tableId key values = return $ case HM.lookup tableId cache of
                                                      (Just table) -> HM.insert key values table
                                                      Nothing -> HM.singleton key values

serializeTable :: Table -> StateT Serialization IO BS.ByteString
serializeTable table = do
  ser <- get
  (r, KVSState _ _ ser' _) <- liftIO $ runStateT (InOut.serializeTable table) $ KVSState undefined undefined ser undefined
  put ser'
  return r

storeTable :: (DB.DB_Iface db) => db -> T.Text -> BS.ByteString -> StateT Stateless IO ()
storeTable db tableId value = liftIO $ evalStateT (InOut.storeTable tableId value) $ KVSState undefined db undefined undefined
