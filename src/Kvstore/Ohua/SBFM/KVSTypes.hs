

module Kvstore.Ohua.SBFM.KVSTypes where

import           Kvstore.KVSTypes
import qualified Kvstore.Ohua.KVSTypes      as KVST

import           Data.StateElement          as SE
import           Data.Dynamic2

-- Cache
loadCacheEntryTableLookUpState = ()
loadCacheEntryTableLookUpStateIdx = KVST.compressTableStateIdx + 1
loadCacheEntryTableCachedState = ()
loadCacheEntryTableCachedStateIdx = KVST.compressTableStateIdx + 2
loadCacheEntryTableWasCachedState = ()
loadCacheEntryTableWasCachedStateIdx = KVST.compressTableStateIdx + 3
loadCacheEntryTableExistsState = ()
loadCacheEntryTableExistsStateIdx = KVST.compressTableStateIdx + 4

-- RequestHandling
serveDestOpState = ()
serveDestOpStateIdx = KVST.compressTableStateIdx + 5
serveDestTableState = ()
serveDestTableStateIdx = KVST.compressTableStateIdx + 6
serveDestKeyState = ()
serveDestKeyStateIdx = KVST.compressTableStateIdx + 7
serveDestFieldsState = ()
serveDestFieldsStateIdx = KVST.compressTableStateIdx + 8
serveDestRecordCountState = ()
serveDestRecordCountStateIdx = KVST.compressTableStateIdx + 9
serveDestValuesState = ()
serveDestValuesStateIdx = KVST.compressTableStateIdx + 10
serveIsReadState = ()
serveIsReadStateIdx = KVST.compressTableStateIdx + 11
serveIsScanState = ()
serveIsScanStateIdx = KVST.compressTableStateIdx + 12
serveIsUpdateState = ()
serveIsUpdateStateIdx = KVST.compressTableStateIdx + 13
serveIsInsertState = ()
serveIsInsertStateIdx = KVST.compressTableStateIdx + 14
deleteTableLookupState = ()
deleteTableLookupStateIdx = KVST.compressTableStateIdx + 15
deleteTableLoadedState = ()
deleteTableLoadedStateIdx = KVST.compressTableStateIdx + 16
deleteComposeResultState = ()
deleteComposeResultStateIdx = KVST.compressTableStateIdx + 17

-- KeyValueService
reqGeneratorState = ()
reqGeneratorStateIdx = KVST.compressTableStateIdx + 18
reqsToListState = ()
reqsToListStateIdx = KVST.compressTableStateIdx + 19
finalResultState = ()
finalResultStateIdx = KVST.compressTableStateIdx + 20


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
