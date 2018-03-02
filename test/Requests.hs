{-# LANGUAGE ImplicitParams #-}

module Requests where

import qualified Data.Text.Lazy            as T
import qualified Data.HashMap.Strict       as HM
import qualified Data.HashSet              as Set
import qualified Data.Vector               as V
import           Control.Monad.State

import           Kvstore.KVSTypes
import           Kvservice_Types

import           ServiceConfig

type ExecReqFn = V.Vector KVRequest -> StateT (KVSState MockDB) IO (V.Vector KVResponse)

insertEntry :: (?execRequests :: ExecReqFn)
            => String -> String -> String -> String -> StateT (KVSState MockDB) IO (V.Vector KVResponse)
insertEntry table key field value = (?execRequests . V.singleton)
                                    $ KVRequest INSERT
                                                (T.pack table)
                                                (T.pack key)
                                                Nothing
                                                Nothing
                                                $ Just $ HM.singleton (T.pack field) (T.pack value)

deleteEntry :: (?execRequests :: ExecReqFn)
            => String -> String -> StateT (KVSState MockDB) IO (V.Vector KVResponse)
deleteEntry table key = (?execRequests . V.singleton) $ KVRequest DELETE
                                                                 (T.pack table)
                                                                 (T.pack key)
                                                                 Nothing
                                                                 Nothing
                                                                 Nothing

updateEntry :: (?execRequests :: ExecReqFn)
           => String -> String -> String -> String -> StateT (KVSState MockDB) IO (V.Vector KVResponse)
updateEntry table key field value = (?execRequests . V.singleton)
                                   $ KVRequest UPDATE
                                               (T.pack table)
                                               (T.pack key)
                                               Nothing
                                               Nothing
                                               $ Just $ HM.singleton (T.pack field) (T.pack value)

readEntry :: (?execRequests :: ExecReqFn)
         => String -> String -> String -> StateT (KVSState MockDB) IO (V.Vector KVResponse)
readEntry table key field = (?execRequests . V.singleton)
                           $ KVRequest READ
                                       (T.pack table)
                                       (T.pack key)
                                       (Just $ Set.fromList [T.pack field])
                                       Nothing
                                       Nothing

scanEntry :: (?execRequests :: ExecReqFn)
         => String -> String -> String -> StateT (KVSState MockDB) IO (V.Vector KVResponse)
scanEntry table key field = (?execRequests . V.singleton)
                           $ KVRequest SCAN
                                       (T.pack table)
                                       (T.pack key)
                                       (Just $ Set.fromList [T.pack field])
                                       (Just 3)
                                       Nothing
