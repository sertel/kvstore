{-# LANGUAGE FlexibleContexts, TupleSections, OverloadedStrings #-}

module Kvstore.Ohua.SBFM.Cache where

import           Kvservice_Types

import qualified Data.Text.Lazy          as T
import qualified Data.ByteString.Lazy    as BS
import           Control.Monad.State
import qualified Data.Vector             as Vector
import qualified Data.HashMap.Strict     as Map
import qualified Data.Set                as Set
import           Data.Maybe

import qualified DB_Iface                as DB
import           Kvstore.KVSTypes
import           Kvstore.InputOutput

import           Debug.Trace

import           Monad.StreamsBasedFreeMonad
import           Monad.StreamsBasedExplicitAPI
import           Data.Dynamic2

import qualified Control.Monad.State     as S

import           Kvstore.Ohua.KVSTypes
import           Kvstore.Ohua.SBFM.KVSTypes
import           Kvstore.Ohua.Cache


  -- algo
prepareCacheEntry :: Var T.Text -> Var (Maybe BS.ByteString) -> ASTM [Dynamic] (Var (Maybe (T.Text, Table)))
prepareCacheEntry tableId serializedValTable = do
    decrypted <-
        liftWithIndexNamed
            decryptTableStateIdx
            "prepare-entry/decrypt"
            (decryptTableSF . fromJust)
            serializedValTable
    decompressed <-
        liftWithIndexNamed
            decompressTableStateIdx
            "prepare-entry/decompress"
            decompressTableSF
            decrypted
    lift2WithIndexNamed
        deserializeTableStateIdx
        "prepare-entry/deserialize"
        (\tId d -> (fmap (Just . (tId, )) . deserializeTableSF) d)
        tableId
        decompressed

loadCacheEntry :: (DB.DB_Iface db, Typeable db)
               => Var KVStore -> Var db -> Var T.Text -> ASTM [Dynamic] (Var (Maybe (T.Text, Table)))
loadCacheEntry kvs db tableId = do
    table <-
        lift2WithIndexNamed
            loadCacheEntryTableLookUpStateIdx
            "load-entry/lookup-state"
            ((S.return .) . Map.lookup :: T.Text -> KVStore -> StateT Stateless IO (Maybe Table))
            tableId
            kvs
    tableCached <-
        liftWithIndexNamed
            loadCacheEntryTableCachedStateIdx
            "load-entry/is-cached"
            (S.return . isJust :: Maybe Table -> StateT Stateless IO Bool)
            table
    if_ tableCached
        (lift2WithIndexNamed
             loadCacheEntryTableWasCachedStateIdx
             "load-entry/was-cached"
             ((\tId t -> S.return $ (tId, ) <$> t) :: T.Text -> Maybe Table -> StateT Stateless IO (Maybe ( T.Text
                                                                                                          , Table)))
             tableId
             table)
        (do serializedValTable <-
                lift2WithIndexNamed
                    loadTableStateIdx
                    "load-entry/load-table"
                    loadTableSF
                    db
                    tableId
            tableExists <-
                liftWithIndexNamed
                    loadCacheEntryTableExistsStateIdx
                    "load-entry/table-exists"
                    (S.return . isJust :: Maybe BS.ByteString -> StateT Stateless IO Bool)
                    serializedValTable
            if_ tableExists (prepareCacheEntry tableId serializedValTable) $
                sfConst' Nothing)
