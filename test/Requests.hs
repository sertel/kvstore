{-# LANGUAGE ImplicitParams #-}

module Requests where

import qualified Data.Text.Lazy            as T
import qualified Data.HashMap.Strict       as HM
import qualified Data.HashSet              as Set
import qualified Data.Vector               as V
import           Data.Int
import           Control.Monad.State

import           Kvstore.KVSTypes
import           Kvservice_Types

import           ServiceConfig
import Versions

type ExecReqFn = ExecReqFn_ MockDB

packFieldsAndValues = Just . HM.fromList . map (\(k,v) -> (T.pack k, T.pack v)) . HM.toList
packFields = Just . Set.fromList . map T.pack . Set.toList

prepareINSERT :: String -> String -> HM.HashMap String String -> KVRequest
prepareINSERT table key fieldsAndValues = KVRequest INSERT
                                              (T.pack table)
                                              (T.pack key)
                                              Nothing
                                              Nothing
                                              $ packFieldsAndValues fieldsAndValues

insertEntry :: (?execRequests :: ExecReqFn)
            => String -> String -> String -> String -> StateT (KVSState MockDB) IO (V.Vector KVResponse)
insertEntry table key field = ?execRequests . V.singleton . prepareINSERT table key . HM.singleton field

insertEntries :: (?execRequests :: ExecReqFn)
            => String -> [(String,String,String)] -> StateT (KVSState MockDB) IO (V.Vector KVResponse)
insertEntries table = ?execRequests . V.fromList . map (\(k,f,v) -> prepareINSERT table k $ HM.singleton f v)


prepareDELETE :: String -> String -> KVRequest
prepareDELETE table key = KVRequest DELETE
                                    (T.pack table)
                                    (T.pack key)
                                    Nothing
                                    Nothing
                                    Nothing

deleteEntry :: (?execRequests :: ExecReqFn)
            => String -> String -> StateT (KVSState MockDB) IO (V.Vector KVResponse)
deleteEntry table = ?execRequests . V.singleton . prepareDELETE table

prepareUPDATE :: String -> String -> HM.HashMap String String -> KVRequest
prepareUPDATE table key fieldsAndValues = KVRequest UPDATE
                                              (T.pack table)
                                              (T.pack key)
                                              Nothing
                                              Nothing
                                              $ packFieldsAndValues fieldsAndValues

updateEntry :: (?execRequests :: ExecReqFn)
           => String -> String -> String -> String -> StateT (KVSState MockDB) IO (V.Vector KVResponse)
updateEntry table key field = ?execRequests . V.singleton . prepareUPDATE table key . HM.singleton field

prepareREAD :: String -> String -> Set.HashSet String -> KVRequest
prepareREAD table key fields = KVRequest READ
                                        (T.pack table)
                                        (T.pack key)
                                        (packFields fields)
                                        Nothing
                                        Nothing

readEntry :: (?execRequests :: ExecReqFn)
         => String -> String -> String -> StateT (KVSState MockDB) IO (V.Vector KVResponse)
readEntry table key field = ?execRequests . V.singleton . prepareREAD table key $ Set.fromList [field]

prepareSCAN :: String -> String -> Int -> Set.HashSet String -> KVRequest
prepareSCAN table key count fields = KVRequest SCAN
                                        (T.pack table)
                                        (T.pack key)
                                        (packFields fields)
                                        (Just $ fromIntegral count)
                                        Nothing

scanEntry :: (?execRequests :: ExecReqFn)
         => String -> String -> String -> StateT (KVSState MockDB) IO (V.Vector KVResponse)
scanEntry table key = ?execRequests . V.singleton . prepareSCAN table key 3 . Set.singleton
