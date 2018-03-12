{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}

module Kvstore.Ohua.SBFM.RequestHandling where

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

import           Monad.StreamsBasedFreeMonad
import           Monad.StreamsBasedExplicitAPI
import           Data.Dynamic2
import           Kvstore.Ohua.KVSTypes
import           Kvstore.Ohua.SBFM.KVSTypes
import           Debug.Trace

import           Kvstore.Ohua.RequestHandling

--
-- algos
--

update :: (DB.DB_Iface db, Typeable db)
       => Var KVStore -> Var db -> Var T.Text -> Var T.Text -> Var (Maybe (HM.HashMap T.Text T.Text))
       -> ASTM [Dynamic] (Var KVResponse)
update cache db tableId key values = do
  table' <- lift4WithIndex calcUpdateStateIdx
                           (\ c t k v -> calculateUpdate c t k $ fromMaybe HM.empty v)
                           cache tableId key values
  serializedTable <- liftWithIndex updateSerializeTableStateIdx
                                   serializeTable table'
  lift3WithIndex updateStoreTableStateIdx
                 (\d t s -> const (KVResponse UPDATE (Just HM.empty) Nothing Nothing) <$> storeTable d t s)
                 db tableId serializedTable

insert :: (DB.DB_Iface db, Typeable db)
       => Var KVStore -> Var db -> Var T.Text -> Var T.Text -> Var (Maybe (HM.HashMap T.Text T.Text))
       -> ASTM [Dynamic] (Var KVResponse)
insert cache db tableId key values = do
  table' <- lift4WithIndex calcInsertStateIdx
                           (\ c t k v -> calculateInsert c t k $ fromMaybe HM.empty v)
                           cache tableId key values
  serializedTable <- liftWithIndex insertSerializeTableStateIdx serializeTable table'
  lift3WithIndex insertStoreTableStateIdx
                 (\d t s -> const (KVResponse INSERT (Just HM.empty) Nothing Nothing) <$> storeTable d t s)
                 db tableId serializedTable

delete :: (DB.DB_Iface db, Typeable db)
       => Var KVStore -> Var db -> Var T.Text -> Var T.Text
       -> ASTM [Dynamic] (Var KVResponse)
delete cache db tableId key = do
  table <- lift2WithIndex deleteTableLookupStateIdx
                          ((return .) . HM.lookup :: T.Text -> KVStore -> StateT Stateless IO (Maybe Table))
                          tableId cache
  tableLoaded <- liftWithIndex deleteTableLoadedStateIdx
                               (return . isJust :: Maybe Table -> StateT Stateless IO Bool)
                               table
  done <- if_ tableLoaded
              (do
                serializedTable <- lift2WithIndex deleteSerializeTableStateIdx
                                                  (\k t -> (serializeTable . HM.delete k . fromJust) t)
                                                  key table
                lift3WithIndex deleteStoreTableStateIdx storeTable db tableId serializedTable)
              (sfConst' ())
  liftWithIndex deleteComposeResultStateIdx
                (const (return $ KVResponse DELETE (Just HM.empty) Nothing Nothing) :: () -> StateT Stateless IO KVResponse)
                done


serve :: (DB.DB_Iface a, Typeable a)
      => Var KVStore -> Var a -> Var KVRequest -> ASTM [Dynamic] (Var KVResponse)
-- serve cache db (KVRequest op tableId key fields recordCount values) = do
serve cache db req = do
  -- does destructuring work?
  op <- liftWithIndex serveDestOpStateIdx
                      (return . kVRequest_op :: KVRequest -> StateT Stateless IO Operation)
                      req
  tableId <- liftWithIndex serveDestTableStateIdx
                      (return . kVRequest_table :: KVRequest -> StateT Stateless IO T.Text)
                      req
  key <- liftWithIndex serveDestKeyStateIdx
                      (return . kVRequest_key :: KVRequest -> StateT () IO T.Text)
                      req
  fields <- liftWithIndex serveDestFieldsStateIdx
                      (return . kVRequest_fields :: KVRequest -> StateT Stateless IO (Maybe (Set.HashSet T.Text)))
                      req
  recordCount <- liftWithIndex serveDestRecordCountStateIdx
                      (return . kVRequest_recordCount :: KVRequest -> StateT Stateless IO (Maybe Int32))
                      req
  values <- liftWithIndex serveDestValuesStateIdx
                      (return . kVRequest_values :: KVRequest -> StateT Stateless IO (Maybe (HM.HashMap T.Text T.Text)))
                      req
  isRead <- liftWithIndex serveIsReadStateIdx (compare READ) op
  if_ isRead
      (lift4WithIndex rEADReqHandlingStateIdx read_ cache tableId key fields)
      (do
        isScan <- liftWithIndex serveIsScanStateIdx (compare SCAN) op
        if_ isScan
          (lift4WithIndex sCANReqHandlingStateIdx scan cache tableId key recordCount)
          (do
            isUpdate <- liftWithIndex serveIsUpdateStateIdx (compare UPDATE) op
            if_ isUpdate
                (update cache db tableId key values)
                (do
                  isInsert <- liftWithIndex serveIsInsertStateIdx (compare INSERT) op
                  if_ isInsert
                      (insert cache db tableId key values)
                      (delete cache db tableId key)
                  )
            )
        )
    where
      compare :: Operation -> Operation -> StateT Stateless IO Bool
      compare op = return . (op ==)
