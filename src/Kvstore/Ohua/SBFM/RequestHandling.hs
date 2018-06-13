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

prepareTable :: Int -> Int -> Int -> Var Table -> ASTM [Dynamic] (Var BS.ByteString)
prepareTable serializeTableStateIdx compressTableStateIdx encryptTableStateIdx table = do
    serializedTable <- liftWithIndex serializeTableStateIdx serializeTable table
    compressedTable <-
        liftWithIndex compressTableStateIdx compressTable serializedTable
    liftWithIndex encryptTableStateIdx encryptTable compressedTable
        -- _ <- liftWithIndex updateStoreTableStateIdx (storeTable db tableId) serializedTable

pureUnitSf :: a -> StateT () IO a
pureUnitSf = pure

writeback :: (DB.DB_Iface db, Typeable db) => Var KVStore -> Var db -> Var (Set.HashSet T.Text) -> ASTM [Dynamic] (Var [()])
writeback store db touched = do
  touchedList <- liftWithIndex writebackTouchedListIdx (pureUnitSf . Set.toList) touched
  smap
    (\t -> do
        table <- lift2WithIndex writebackGetTableIdx (\t store' -> pureUnitSf $ store' HM.! t) t store
        preparedTable <- prepareTable writebackSerializeTableStateIdx writebackCompressTableStateIdx writebackEncryptTableStateIdx table
        lift3WithIndex writebackStoreTableStateIdx storeTable db t preparedTable
        )
    touchedList


serve :: (DB.DB_Iface a, Typeable a)
      => Var KVStore -> Var a -> Var KVRequest -> ASTM [Dynamic] (Var KVResponse)
-- serve cache db (KVRequest op tableId key fields recordCount values) = do
serve cache db req
  -- does destructuring work?
 = do
    op <-
        liftWithIndex
            serveDestOpStateIdx
            (return . kVRequest_op :: KVRequest -> StateT Stateless IO Operation)
            req
    tableId <-
        liftWithIndex
            serveDestTableStateIdx
            (return . kVRequest_table :: KVRequest -> StateT Stateless IO T.Text)
            req
    key <-
        liftWithIndex
            serveDestKeyStateIdx
            (return . kVRequest_key :: KVRequest -> StateT () IO T.Text)
            req
    fields <-
        liftWithIndex
            serveDestFieldsStateIdx
            (return . kVRequest_fields :: KVRequest -> StateT Stateless IO (Maybe (Set.HashSet T.Text)))
            req
    recordCount <-
        liftWithIndex
            serveDestRecordCountStateIdx
            (return . kVRequest_recordCount :: KVRequest -> StateT Stateless IO (Maybe Int32))
            req
    values <-
        liftWithIndex
            serveDestValuesStateIdx
            (return . kVRequest_values :: KVRequest -> StateT Stateless IO (Maybe (HM.HashMap T.Text T.Text)))
            req
    isRead <- liftWithIndex serveIsReadStateIdx (compare READ) op
    if_ isRead
        (lift4WithIndex rEADReqHandlingStateIdx read_ cache tableId key fields)
        (do isScan <- liftWithIndex serveIsScanStateIdx (compare SCAN) op
            if_
                isScan
                (lift4WithIndex
                     sCANReqHandlingStateIdx
                     scan
                     cache
                     tableId
                     key
                     recordCount)
                (do isUpdate <-
                        liftWithIndex serveIsUpdateStateIdx (compare UPDATE) op
                    if_
                        isUpdate
                        (stdResponse UPDATE)
                        (do isInsert <-
                                liftWithIndex
                                    serveIsInsertStateIdx
                                    (compare INSERT)
                                    op
                            if_
                                isInsert
                                (stdResponse INSERT)
                                (stdResponse DELETE))))
  where
    stdResponse ty =
        sfConst
            (KVResponse ty (Just mempty) Nothing Nothing :: KVResponse)
    compare :: Operation -> Operation -> StateT Stateless IO Bool
    compare op = return . (op ==)
