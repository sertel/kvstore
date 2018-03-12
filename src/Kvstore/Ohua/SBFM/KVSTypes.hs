

module Kvstore.Ohua.SBFM.KVSTypes where

import           Kvstore.KVSTypes
import qualified Kvstore.Ohua.KVSTypes      as KVST

import           Data.StateElement          as SE
import           Data.Dynamic2

-- Cache
loadCacheEntryTableLookUpState = ()
loadCacheEntryTableLookUpStateIdx = 15 :: Int
loadCacheEntryTableCachedState = ()
loadCacheEntryTableCachedStateIdx = 16 :: Int
loadCacheEntryTableWasCachedState = ()
loadCacheEntryTableWasCachedStateIdx = 17 :: Int
loadCacheEntryTableExistsState = ()
loadCacheEntryTableExistsStateIdx = 18 :: Int

-- RequestHandling
serveDestOpState = ()
serveDestOpStateIdx = 19 :: Int
serveDestTableState = ()
serveDestTableStateIdx = 20 :: Int
serveDestKeyState = ()
serveDestKeyStateIdx = 21 :: Int
serveDestFieldsState = ()
serveDestFieldsStateIdx = 22 :: Int
serveDestRecordCountState = ()
serveDestRecordCountStateIdx = 23 :: Int
serveDestValuesState = ()
serveDestValuesStateIdx = 24 :: Int
serveIsReadState = ()
serveIsReadStateIdx = 25 :: Int
serveIsScanState = ()
serveIsScanStateIdx = 26 :: Int
serveIsUpdateState = ()
serveIsUpdateStateIdx = 27 :: Int
serveIsInsertState = ()
serveIsInsertStateIdx = 28 :: Int
deleteTableLookupState = ()
deleteTableLookupStateIdx = 29 :: Int
deleteTableLoadedState = ()
deleteTableLoadedStateIdx = 30 :: Int
deleteComposeResultState = ()
deleteComposeResultStateIdx = 31 :: Int

-- KeyValueService
reqGeneratorState = ()
reqGeneratorStateIdx = 32 :: Int
reqsToListState = ()
reqsToListStateIdx = 33 :: Int
finalResultState = ()
finalResultStateIdx = 34 :: Int


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

globalState :: Serialization -> Deserialization -> [Dynamic]
globalState ser deser = map (\(SE.S _ d) -> d) (KVST.globalState ser deser) ++ additionalGlobalState
