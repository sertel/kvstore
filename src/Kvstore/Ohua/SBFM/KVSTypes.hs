

module Kvstore.Ohua.SBFM.KVSTypes where

import           Kvstore.KVSTypes
import qualified Kvstore.Ohua.KVSTypes      as KVST

import           Data.StateElement          as SE
import           Data.Dynamic2

loadCacheEntryTableLookUpState = ()
loadCacheEntryTableLookUpStateIdx = 15 :: Int
loadCacheEntryTableCachedState = ()
loadCacheEntryTableCachedStateIdx = 16 :: Int
loadCacheEntryTableWasCachedState = ()
loadCacheEntryTableWasCachedStateIdx = 17 :: Int
loadCacheEntryTableExistsState = ()
loadCacheEntryTableExistsStateIdx = 18 :: Int

additionalGlobalState = [
                          toDyn loadCacheEntryTableLookUpState
                        , toDyn loadCacheEntryTableCachedState
                        , toDyn loadCacheEntryTableWasCachedState
                        , toDyn loadCacheEntryTableExistsState
                        ]

globalState :: Serialization -> Deserialization -> [Dynamic]
globalState ser deser = map (\(SE.S _ d) -> d) (KVST.globalState ser deser) ++ additionalGlobalState
