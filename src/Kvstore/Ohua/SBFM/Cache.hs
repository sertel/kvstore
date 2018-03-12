{-# LANGUAGE FlexibleContexts, TupleSections #-}

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
loadCacheEntry :: (DB.DB_Iface db, Typeable db)
               => Var KVStore -> Var db -> Var T.Text -> ASTM [Dynamic] (Var (Maybe (T.Text, Table)))
loadCacheEntry kvs db tableId = do
    table <- lift2WithIndex loadCacheEntryTableLookUpStateIdx
                            ((S.return .) . Map.lookup :: T.Text -> KVStore -> StateT Stateless IO (Maybe Table))
                            tableId kvs
    tableCached <- liftWithIndex loadCacheEntryTableCachedStateIdx
                                 (S.return . isJust :: Maybe Table -> StateT Stateless IO Bool)
                                 table
    if_ tableCached
        (lift2WithIndex loadCacheEntryTableWasCachedStateIdx
                        ((\tId t -> S.return $ (tId,) <$> t) :: T.Text -> Maybe Table -> StateT Stateless IO (Maybe (T.Text, Table)))
                        tableId table)
        (do
          serializedValTable <- lift2WithIndex loadTableStateIdx
                                               loadTableSF db tableId
          tableExists <- liftWithIndex loadCacheEntryTableExistsStateIdx
                                       (S.return . isJust :: Maybe BS.ByteString -> StateT Stateless IO Bool)
                                       serializedValTable
          if_ tableExists
             (lift2WithIndex deserializeTableStateIdx
                          (\tId svt -> (fmap (Just . (tId,)) . deserializeTableSF . fromJust) svt) tableId serializedValTable)
             $ sfConst' Nothing)
