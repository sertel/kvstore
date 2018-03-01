{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Kvstore.Ohua.FBM.KVSTypes where

import           Kvservice_Types
import           Kvstore.JSONSerialization
import           Kvstore.KVSTypes
import qualified Control.DeepSeq            as DS
import           GHC.Generics
import qualified DB_Iface                   as DB
import           FuturesBasedMonad
import           Data.Typeable

data LocalState serde = Stateless | Serializer serde | Deserializer serde deriving Generic

instance DS.NFData Operation
instance DS.NFData KVResponse

instance DS.NFData JSONSerDe
instance DS.NFData serde => DS.NFData (LocalState serde)

--
-- Global State definition
--

foldIntoCacheState = Stateless
foldIntoCacheStateIdx = 0 :: Int
foldEvictFromCacheState = Stateless
foldEvictFromCacheStateIdx = 1 :: Int
deserializeTableState = Deserializer
deserializeTableStateIdx = 2 :: Int
rEADReqHandlingState = Stateless
rEADReqHandlingStateIdx = 3 :: Int
sCANReqHandlingState = Stateless
sCANReqHandlingStateIdx = 4 :: Int
calcUpdateState = Stateless
calcUpdateStateIdx = 5 :: Int
calcInsertState = Stateless
calcInsertStateIdx = 6 :: Int
updateSerializeTableState = Serializer
updateSerializeTableStateIdx = 7 :: Int
insertSerializeTableState = Serializer
insertSerializeTableStateIdx = 8 :: Int
deleteSerializeTableState = Serializer
deleteSerializeTableStateIdx = 9 :: Int
calcDeleteState = Stateless
calcDeleteStateIdx = 10 :: Int
insertStoreTableState = Stateless
insertStoreTableStateIdx = 11 :: Int
deleteStoreTableState = Stateless
deleteStoreTableStateIdx = 12 :: Int
updateStoreTableState = Stateless
updateStoreTableStateIdx = 13 :: Int

globalState :: forall serde.(DS.NFData serde, Typeable serde) => serde -> [FuturesBasedMonad.S]
globalState serde = [ toS (foldIntoCacheState :: LocalState serde)
                    , toS (foldEvictFromCacheState :: LocalState serde)
                    , toS (deserializeTableState serde :: LocalState serde)
                    , toS (rEADReqHandlingState :: LocalState serde)
                    , toS (sCANReqHandlingState :: LocalState serde)
                    , toS (calcUpdateState :: LocalState serde)
                    , toS (calcInsertState :: LocalState serde)
                    , toS $ updateSerializeTableState serde
                    , toS $ insertSerializeTableState serde
                    , toS $ deleteSerializeTableState serde
                    , toS (calcDeleteState :: LocalState serde)
                    , toS (insertStoreTableState :: LocalState serde)
                    , toS (deleteStoreTableState :: LocalState serde)
                    ]
