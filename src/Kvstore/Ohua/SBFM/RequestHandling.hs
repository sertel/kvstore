{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes, OverloadedStrings #-}

module Kvstore.Ohua.SBFM.RequestHandling where

import           Data.Int
import qualified Data.Text.Lazy             as T
import qualified Data.HashSet               as Set
import qualified Data.HashMap.Strict     as HM
import qualified Data.ByteString.Lazy    as BS
import           Data.Maybe
import           Control.Monad.State
import           Control.Monad.IO.Class
import Control.DeepSeq

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
import           Kvstore.Ohua.Cache

--
-- algos
--

prepareTable :: Int -> Int -> Int -> Var Table -> ASTM [Dynamic] (Var BS.ByteString)
prepareTable serializeTableStateIdx compressTableStateIdx encryptTableStateIdx table = do
    serializedTable <-
        liftWithIndexNamed
            serializeTableStateIdx
            "prepare-store/serialize"
            serializeTableSF
            table
    compressedTable <-
        liftWithIndexNamed
            compressTableStateIdx
            "prepare-store/compress"
            compressTableSF
            serializedTable
    -- return compressedTable
    liftWithIndexNamed
        encryptTableStateIdx
        "prepare-store/encrypt"
        encryptTableSF
        compressedTable
        -- _ <- liftWithIndex updateStoreTableStateIdx (storeTable db tableId) serializedTable

pure' :: (Applicative m, NFData a) => a -> m a
pure' a = a `deepseq` pure a

pureUnitSf :: NFData a => a -> StateT () IO a
pureUnitSf = pure'
pureUnitSfLazy :: a -> StateT () IO a
pureUnitSfLazy = pure

writeback :: (DB.DB_Iface db, Typeable db) => Var KVStore -> Var db -> Var (Set.HashSet T.Text) -> ASTM [Dynamic] (Var [()])
writeback store db touched = do
    touchedList <-
        liftWithIndexNamed
            writebackTouchedListIdx
            "writeback/touched-list"
            (pureUnitSf . Set.toList)
            touched
    smap
        (\t -> do
             table <-
                 lift2WithIndexNamed
                     writebackGetTableIdx
                     "writeback/get-table"
                     (\t store' -> pureUnitSf $ store' HM.! t)
                     t
                     store
             preparedTable <-
                 prepareTable
                     writebackSerializeTableStateIdx
                     writebackCompressTableStateIdx
                     writebackEncryptTableStateIdx
                     table
             lift3WithIndexNamed
                 writebackStoreTableStateIdx
                 "writeback/store-table"
                 storeTable
                 db
                 t
                 preparedTable)
        touchedList


serve :: (DB.DB_Iface a, Typeable a)
      => Var KVStore -> Var a -> Var KVRequest -> ASTM [Dynamic] (Var KVResponse)
-- serve cache db (KVRequest op tableId key fields recordCount values) = do
serve cache db req
  -- does destructuring work?
 = do
    op <-
        liftWithIndexNamed
            serveDestOpStateIdx
            "serve/get-request-op"
            (pure' . kVRequest_op :: KVRequest -> StateT Stateless IO Operation)
            req
    tableId <-
        liftWithIndexNamed
            serveDestTableStateIdx
            "serve/get-request-table"
            (pure' . kVRequest_table :: KVRequest -> StateT Stateless IO T.Text)
            req
    key <-
        liftWithIndexNamed
            serveDestKeyStateIdx
            "serve/get-request-key"
            (pure' . kVRequest_key :: KVRequest -> StateT Stateless IO T.Text)
            req
    fields <-
        liftWithIndexNamed
            serveDestFieldsStateIdx
            "serve/get-request-fields"
            (pure' . kVRequest_fields :: KVRequest -> StateT Stateless IO (Maybe (Set.HashSet T.Text)))
            req
    recordCount <-
        liftWithIndexNamed
            serveDestRecordCountStateIdx
            "serve/get-request-record-count"
            (pure' . kVRequest_recordCount :: KVRequest -> StateT Stateless IO (Maybe Int32))
            req
    values <-
        liftWithIndexNamed
            serveDestValuesStateIdx
            "serve/get-request-values"
            (pure' . kVRequest_values :: KVRequest -> StateT Stateless IO (Maybe (HM.HashMap T.Text T.Text)))
            req
    isRead <-
        liftWithIndexNamed
            serveIsReadStateIdx
            "serve/is-read"
            (compare READ)
            op
    if_ isRead
        (lift4WithIndexNamed
             rEADReqHandlingStateIdx
             "serve/handle-read"
             read_
             cache
             tableId
             key
             fields)
        (do isScan <-
                liftWithIndexNamed
                    serveIsScanStateIdx
                    "serve/is-scan"
                    (compare SCAN)
                    op
            if_
                isScan
                (lift4WithIndexNamed
                     sCANReqHandlingStateIdx
                     "serve/handle-scan"
                     scan
                     cache
                     tableId
                     key
                     recordCount)
                (do isUpdate <-
                        liftWithIndexNamed
                            serveIsUpdateStateIdx
                            "serve/is-update"
                            (compare UPDATE)
                            op
                    if_
                        isUpdate
                        (stdResponse UPDATE)
                        (do isInsert <-
                                liftWithIndexNamed
                                    serveIsInsertStateIdx
                                    "serve/is-insert"
                                    (compare INSERT)
                                    op
                            if_
                                isInsert
                                (stdResponse INSERT)
                                (stdResponse DELETE))))
  where
    stdResponse ty =
        sfConst (KVResponse ty (Just mempty) Nothing Nothing :: KVResponse)
    compare :: Operation -> Operation -> StateT Stateless IO Bool
    compare op = pure' . (op ==)
