

module Kvstore.Ohua.SBFM.KVSTypes where

import           Kvstore.KVSTypes
import qualified Kvstore.Ohua.KVSTypes      as KVST

import           Data.StateElement          as SE
import           Data.Dynamic2

-- Cache
loadCacheEntryTableLookUpState = ()
loadCacheEntryTableLookUpStateIdx = KVST.lastStateIdx + 1
loadCacheEntryTableCachedState = ()
loadCacheEntryTableCachedStateIdx = KVST.lastStateIdx + 2
loadCacheEntryTableWasCachedState = ()
loadCacheEntryTableWasCachedStateIdx = KVST.lastStateIdx + 3
loadCacheEntryTableExistsState = ()
loadCacheEntryTableExistsStateIdx = KVST.lastStateIdx + 4

-- RequestHandling
serveDestOpState = ()
serveDestOpStateIdx = KVST.lastStateIdx + 5
serveDestTableState = ()
serveDestTableStateIdx = KVST.lastStateIdx + 6
serveDestKeyState = ()
serveDestKeyStateIdx = KVST.lastStateIdx + 7
serveDestFieldsState = ()
serveDestFieldsStateIdx = KVST.lastStateIdx + 8
serveDestRecordCountState = ()
serveDestRecordCountStateIdx = KVST.lastStateIdx + 9
serveDestValuesState = ()
serveDestValuesStateIdx = KVST.lastStateIdx + 10
serveIsReadState = ()
serveIsReadStateIdx = KVST.lastStateIdx + 11
serveIsScanState = ()
serveIsScanStateIdx = KVST.lastStateIdx + 12
serveIsUpdateState = ()
serveIsUpdateStateIdx = KVST.lastStateIdx + 13
serveIsInsertState = ()
serveIsInsertStateIdx = KVST.lastStateIdx + 14
deleteTableLookupState = ()
deleteTableLookupStateIdx = KVST.lastStateIdx + 15
deleteTableLoadedState = ()
deleteTableLoadedStateIdx = KVST.lastStateIdx + 16
deleteComposeResultState = ()
deleteComposeResultStateIdx = KVST.lastStateIdx + 17

-- KeyValueService
reqGeneratorState = ()
reqGeneratorStateIdx = KVST.lastStateIdx + 18
reqsToListState = ()
reqsToListStateIdx = KVST.lastStateIdx + 19
finalResultState = ()
finalResultStateIdx = KVST.lastStateIdx + 20


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
