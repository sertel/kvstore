
module Kvstore.Ohua.SBFM.KVSTypes where

import           Kvstore.KVSTypes
import qualified Kvstore.Ohua.KVSTypes      as KVST

import           Data.StateElement          as SE
import           Data.Dynamic2

import           Debug.Trace

-- Cache
loadCacheEntryTableLookUpState = ()
loadCacheEntryTableLookUpStateIdx = KVST.lastStateIdx + 1 :: Int
loadCacheEntryTableCachedState = ()
loadCacheEntryTableCachedStateIdx = KVST.lastStateIdx + 2 :: Int
loadCacheEntryTableWasCachedState = ()
loadCacheEntryTableWasCachedStateIdx = KVST.lastStateIdx + 3 :: Int
loadCacheEntryTableExistsState = ()
loadCacheEntryTableExistsStateIdx = KVST.lastStateIdx + 4 :: Int

-- RequestHandling
serveDestOpState = ()
serveDestOpStateIdx = KVST.lastStateIdx + 5 :: Int
serveDestTableState = ()
serveDestTableStateIdx = KVST.lastStateIdx + 6 :: Int
serveDestKeyState = ()
serveDestKeyStateIdx = KVST.lastStateIdx + 7 :: Int
serveDestFieldsState = ()
serveDestFieldsStateIdx = KVST.lastStateIdx + 8 :: Int
serveDestRecordCountState = ()
serveDestRecordCountStateIdx = KVST.lastStateIdx + 9 :: Int
serveDestValuesState = ()
serveDestValuesStateIdx = KVST.lastStateIdx + 10 :: Int
serveIsReadState = ()
serveIsReadStateIdx = KVST.lastStateIdx + 11 :: Int
serveIsScanState = ()
serveIsScanStateIdx = KVST.lastStateIdx + 12 :: Int
serveIsUpdateState = ()
serveIsUpdateStateIdx = KVST.lastStateIdx + 13 :: Int
serveIsInsertState = ()
serveIsInsertStateIdx = KVST.lastStateIdx + 14 :: Int
deleteTableLookupState = ()
deleteTableLookupStateIdx = KVST.lastStateIdx + 15 :: Int
deleteTableLoadedState = ()
deleteTableLoadedStateIdx = KVST.lastStateIdx + 16 :: Int
deleteComposeResultState = ()
deleteComposeResultStateIdx = KVST.lastStateIdx + 17 :: Int

-- KeyValueService
reqGeneratorState = ()
reqGeneratorStateIdx = KVST.lastStateIdx + 18 :: Int
reqsToListState = ()
reqsToListStateIdx = KVST.lastStateIdx + 19 :: Int
finalResultState = ()
finalResultStateIdx = KVST.lastStateIdx + 20 :: Int


additionalGlobalState = [
                          -- Cache
                          toDyn loadCacheEntryTableLookUpState
                        , toDyn loadCacheEntryTableCachedState
                        , toDyn loadCacheEntryTableWasCachedState
                        , toDyn loadCacheEntryTableExistsState

                        -- RequestHandling
                        , toDyn serveDestOpState
                        , toDyn serveDestTableState
                        , toDyn serveDestKeyState
                        , toDyn serveDestFieldsState
                        , toDyn serveDestRecordCountState
                        , toDyn serveDestValuesState
                        , toDyn serveIsReadState
                        , toDyn serveIsScanState
                        , toDyn serveIsUpdateState
                        , toDyn serveIsInsertState
                        , toDyn deleteTableLookupState
                        , toDyn deleteTableLoadedState
                        , toDyn deleteComposeResultState

                        -- KeyValueService
                        , toDyn reqGeneratorState
                        , toDyn reqsToListState
                        , toDyn finalResultState

                        ]

globalState :: Serialization -> Deserialization ->
               Compression -> Decompression ->
               Encryption -> Decryption ->
               [Dynamic]
globalState ser deser comp decomp enc dec =
    map (\(SE.S _ d) -> d) (KVST.globalState ser deser comp decomp enc dec) ++ additionalGlobalState
